# -*- coding: utf-8 -*-
import base64
import json
from datetime import timedelta
from typing import Any, Dict, List

import firebase_admin
import requests
from firebase_admin import credentials, firestore
from google.cloud.firestore import GeoPoint
from google.cloud.firestore_v1.client import Client as FirestoreClient
from googlemaps import Client as GoogleMapsClient
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.logging import log
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon

from pipelines.mapa_realizacoes.infopref.utils import (
    get_bairro_from_lat_long,
    to_camel_case,
    to_snake_case,
)
from pipelines.utils import authenticated_task as task


@task
def get_gmaps_key(secret_name: str = "GMAPS_KEY") -> str:
    """
    Get the Google Maps API key.

    Returns:
        str: The Google Maps API key.
    """
    return get_secret(secret_name)[secret_name]


@task
def get_infopref_headers(token_secret: str = "INFOPREF_TOKEN") -> Dict[str, str]:
    """
    Get the headers for the infopref API.

    Returns:
        Dict[str, str]: The headers.
    """
    token = get_secret(token_secret)[token_secret]
    return {"Authorization": f"Basic {token}"}


@task
def get_infopref_url(url_secret: str = "INFOPREF_URL") -> str:
    """
    Get the URL for the infopref API.

    Returns:
        str: The URL.
    """
    return get_secret(url_secret)[url_secret]


@task
def get_infopref_data(url, headers) -> List[Dict[str, Any]]:
    """
    Get the data from the infopref API.

    Args:
        url (str): The URL.
        headers (Dict[str, str]): The headers.

    Returns:
        List[Dict[str, Any]]: The data.
    """
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    response_json = response.json()
    if not response_json["status"] == "success":
        raise ValueError(f"API returned status {response_json['status']}.")
    data = response_json["data"]
    return data


@task
def load_firestore_credential_to_file(secret_name: str = "FIRESTORE_CREDENTIALS") -> None:
    """
    Load the Firestore credential to a file.

    Args:
        secret_name (str): The secret name.
    """
    secret = get_secret(secret_name)[secret_name]
    credentials = base64.b64decode(secret).decode()
    with open("/tmp/firebase-credentials.json", "w") as f:
        f.write(credentials)


@task(checkpoint=False)
def get_firestore_client() -> FirestoreClient:
    """
    Get the Firestore client.

    Returns:
        FirestoreClient: The Firestore client.
    """
    cred = credentials.Certificate("/tmp/firebase-credentials.json")
    app = firebase_admin.initialize_app(cred)
    return firestore.client(app)


@task(checkpoint=False)
def get_bairros_with_geometry(db: FirestoreClient) -> List[Dict[str, Any]]:
    bairros = [doc.to_dict() for doc in db.collection("bairro").stream()]
    for bairro in bairros:
        bairro["geo"] = json.loads(bairro["geo"])
        if bairro["geo"]["geometry"]["type"] == "Polygon":
            bairro["polygon"] = Polygon(bairro["geo"]["geometry"]["coordinates"][0])
        elif bairro["geo"]["geometry"]["type"] == "MultiPolygon":
            bairro["polygon"] = MultiPolygon(
                [Polygon(coords[0]) for coords in bairro["geo"]["geometry"]["coordinates"]]
            )
        else:
            raise ValueError(f"Invalid geometry type {bairro['geo']['geometry']['type']}")
    return bairros


@task(
    max_retries=3,
    retry_delay=timedelta(seconds=5),
)
def transform_infopref_to_firebase(
    entry: Dict[str, Any], gmaps_key: str, db: FirestoreClient, bairros: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Transform the infopref data to the firebase format.

    Args:
        entry (Dict[str, Any]): The entry.

    Returns:
        Dict[str, Any]: The transformed entry.
    """
    # Check if all input keys are present
    expected_keys = [
        "populacao_beneficiada",  # future cariocas_atendidos
        "logradouro",  # we'll need to geolocate this
        "entrega_projeto",  # future data_fim
        "inicio_projeto",  # future data_inicio
        "descricao_projeto",  # future descricao
        "bairro",  # future id_bairro
        "status",  # future id_status
        "titulo",  # future nome and image_folder
        "investimento",  # future investimento
        "orgao_extenso",  # future id_orgao
        "programa",  # future id_programa
        "tema",  # future id_tema
    ]
    for key in expected_keys:
        if key not in entry:
            raise ValueError(f"Key {key} not found in entry.")

    # Transform fields
    cariocas_atendidos = (
        int(entry["populacao_beneficiada"]) if entry["populacao_beneficiada"] else 0
    )
    gmaps_client: GoogleMapsClient = GoogleMapsClient(key=gmaps_key)
    full_address = f"{entry['logradouro']}, {entry['bairro']}, Rio de Janeiro, Brazil"
    geocode_result = gmaps_client.geocode(full_address)
    # If we fail to geo-locate the address, we use the neighborhood as a fallback
    if not geocode_result:
        log(f"Could not geocode address {full_address}. Falling back to neighborhood.", "warning")
        full_address = f"{entry['bairro']}, Rio de Janeiro, Brazil"
        geocode_result = gmaps_client.geocode(full_address)
        # If we still fail, we raise an error
        if not geocode_result:
            raise ValueError(f"Could not geocode address {full_address}.")
    latitude = geocode_result[0]["geometry"]["location"]["lat"]
    longitude = geocode_result[0]["geometry"]["location"]["lng"]
    coords = GeoPoint(latitude, longitude)
    data_fim = entry["entrega_projeto"]
    data_inicio = entry["inicio_projeto"]
    descricao = entry["descricao_projeto"]
    id_bairro = to_snake_case(entry["bairro"])
    id_status = to_snake_case(entry["status"])
    image_folder = to_snake_case(entry["titulo"])
    investimento = float(entry["investimento"]) if entry["investimento"] else 0
    nome = entry["titulo"]
    id_orgao = to_snake_case(entry["orgao_extenso"])
    id_programa = to_snake_case(entry["programa"])
    id_tema = to_snake_case(entry["tema"])

    # Get fields from related collections
    # - id_subprefeitura: in the "bairro" collection, find the document that matches the id_bairro
    #   and get the id_subprefeitura field
    # - id_cidade: in the "subprefeitura" collection, find the document that matches the
    #   id_subprefeitura and get the id_cidade field
    all_id_bairros = [doc.id for doc in db.collection("bairro").stream()]
    id_subprefeitura: str = None
    id_cidade: str = None
    if id_bairro not in all_id_bairros:
        try:
            bairro = get_bairro_from_lat_long(coords.latitude, coords.longitude, bairros)
            id_bairro = to_snake_case(bairro["nome"])
            if id_bairro not in all_id_bairros:
                raise ValueError(f"Could not find bairro with id {id_bairro}.")
            entry["id_bairro"] = id_bairro
        except ValueError:
            id_bairro = None
            log(f"Could not find bairro for ({coords.latitude},{coords.longitude}).", "warning")
    else:
        bairro_doc = db.collection("bairro").document(id_bairro).get()
        id_subprefeitura = bairro_doc.to_dict()["id_subprefeitura"]
        subprefeitura_doc = db.collection("subprefeitura").document(id_subprefeitura).get()
        if not subprefeitura_doc.exists:
            log(f"Could not find subprefeitura with id {id_subprefeitura}.", "warning")
        else:
            id_cidade = subprefeitura_doc.to_dict()["id_cidade"]

    output = {
        "cariocas_atendidos": cariocas_atendidos,
        "coords": coords,
        "data_fim": data_fim,
        "data_inicio": data_inicio,
        "descricao": descricao,
        "id_bairro": id_bairro,
        "id_cidade": id_cidade,
        "id_orgao": id_orgao,
        "id_programa": id_programa,
        "id_status": id_status,
        "id_subprefeitura": id_subprefeitura,
        "id_tema": id_tema,
        "id_tipo": "obra",
        "image_folder": image_folder,
        "investimento": investimento,
        "nome": nome,
    }
    return output


@task
def upload_infopref_data_to_firestore(data: List[Dict[str, Any]], db: FirestoreClient) -> None:
    """
    Upload the infopref data to Firestore.

    Args:
        data (List[Dict[str, Any]]): The data.
    """
    # Get all id_bairro, id_status, id_orgao, id_programa, id_tema from Firestore
    all_id_bairros = [doc.id for doc in db.collection("bairro").stream()]
    all_id_cidades = [doc.id for doc in db.collection("cidade").stream()]
    all_id_orgaos = [doc.id for doc in db.collection("orgao").stream()]
    all_id_programas = [doc.id for doc in db.collection("programa").stream()]
    all_id_status = [doc.id for doc in db.collection("status").stream()]
    all_id_subprefeituras = [doc.id for doc in db.collection("subprefeitura").stream()]
    all_id_temas = [doc.id for doc in db.collection("tema").stream()]

    # Check if all IDs from the input data are present in Firestore
    batch = db.batch()
    batch_len = 0
    is_ok = True
    ok_entries = []
    not_found_bairros = {}
    not_found_cidades = {}
    not_found_status = {}
    not_found_subprefeituras = {}
    not_found_orgaos = {}
    for entry in data:
        is_ok = True
        if entry["id_bairro"] not in all_id_bairros:
            if entry["id_bairro"] not in not_found_bairros:
                not_found_bairros[entry["id_bairro"]] = 0
            not_found_bairros[entry["id_bairro"]] += 1
            is_ok = False
        if entry["id_cidade"] not in all_id_cidades:
            if entry["id_cidade"] not in not_found_cidades:
                not_found_cidades[entry["id_cidade"]] = 0
            not_found_cidades[entry["id_cidade"]] += 1
            is_ok = False
        if entry["id_status"] not in all_id_status:
            if entry["id_status"] not in not_found_status:
                not_found_status[entry["id_status"]] = 0
            not_found_status[entry["id_status"]] += 1
            is_ok = False
        if entry["id_subprefeitura"] not in all_id_subprefeituras:
            if entry["id_subprefeitura"] not in not_found_subprefeituras:
                not_found_subprefeituras[entry["id_subprefeitura"]] = 0
            not_found_subprefeituras[entry["id_subprefeitura"]] += 1
            is_ok = False
        if entry["id_orgao"] not in all_id_orgaos:
            if entry["id_orgao"] not in not_found_orgaos:
                not_found_orgaos[entry["id_orgao"]] = 0
            not_found_orgaos[entry["id_orgao"]] += 1
            is_ok = False
        if entry["id_programa"] not in all_id_programas:
            batch.set(
                db.collection("programa").document(entry["id_programa"]),
                {"nome": to_camel_case(entry["id_programa"]), "descricao": ""},
            )
            batch_len += 1
            if batch_len >= 450:  # 500 is the limit for batch writes, we'll use 450 to be safe
                batch.commit()
                batch = db.batch()
                batch_len = 0
        if entry["id_tema"] not in all_id_temas:
            batch.set(
                db.collection("tema").document(entry["id_tema"]),
                {"nome": to_camel_case(entry["id_tema"])},
            )
            batch_len += 1
            if batch_len >= 450:  # 500 is the limit for batch writes, we'll use 450 to be safe
                batch.commit()
                batch = db.batch()
                batch_len = 0
        if is_ok:
            ok_entries.append(entry)
        if batch_len > 0:
            batch.commit()
            batch = db.batch()
            batch_len = 0

    log(f"Length of all entries: {len(data)}")
    log(f"Length of ok entries: {len(ok_entries)}")
    log(f"Could not find the following bairros: {not_found_bairros}.", "warning")
    log(f"Could not find the following cidades: {not_found_cidades}.", "warning")
    log(f"Could not find the following status: {not_found_status}.", "warning")
    log(f"Could not find the following subprefeituras: {not_found_subprefeituras}.", "warning")
    log(f"Could not find the following orgaos: {not_found_orgaos}.", "warning")

    # Upload data to Firestore. This will be done in a few steps:
    # - First we have to add the document to the `realizacao` collection. This will have all the
    # fields from the input data except for `id_orgao`, `id_programa` and `id_tema`.
    # - Then we have to add the document to the `realizacao_orgao`, `realizacao_programa` and
    # `realizacao_tema` collections. These collections will have the `realizacao_id` and the
    # respective `id_orgao`, `id_programa` and `id_tema`.
    batch = db.batch()
    for entry in ok_entries:
        # Add document to `realizacao` collection
        id_realizacao = to_snake_case(entry["nome"]).replace("/", "-")
        data = {
            "cariocas_atendidos": entry["cariocas_atendidos"],
            "coords": entry["coords"],
            "data_fim": entry["data_fim"],
            "data_inicio": entry["data_inicio"],
            "descricao": entry["descricao"],
            "id_bairro": entry["id_bairro"],
            "id_cidade": entry["id_cidade"],
            "id_status": entry["id_status"],
            "id_subprefeitura": entry["id_subprefeitura"],
            "id_tipo": entry["id_tipo"],
            "image_folder": entry["image_folder"],
            "investimento": entry["investimento"],
            "nome": entry["nome"],
        }
        batch.set(db.collection("realizacao").document(id_realizacao), data)
        batch_len += 1

        if batch_len >= 450:  # 500 is the limit for batch writes, we'll use 450 to be safe
            batch.commit()
            batch = db.batch()
            batch_len = 0

        # Add document to `realizacao_orgao` collection
        if entry["id_orgao"]:
            id_document = f"{id_realizacao}__{entry['id_orgao']}"
            data = {"id_realizacao": id_realizacao, "id_orgao": entry["id_orgao"]}
            batch.set(db.collection("realizacao_orgao").document(id_document), data)
            batch_len += 1

            if batch_len >= 450:  # 500 is the limit for batch writes, we'll use 450 to be safe
                batch.commit()
                batch = db.batch()
                batch_len = 0

        # Add document to `realizacao_programa` collection
        if entry["id_programa"]:
            id_document = f"{id_realizacao}__{entry['id_programa']}"
            data = {"id_realizacao": id_realizacao, "id_programa": entry["id_programa"]}
            batch.set(db.collection("realizacao_programa").document(id_document), data)
            batch_len += 1

            if batch_len >= 450:  # 500 is the limit for batch writes, we'll use 450 to be safe
                batch.commit()
                batch = db.batch()
                batch_len = 0

        # Add document to `realizacao_tema` collection
        if entry["id_tema"]:
            id_document = f"{id_realizacao}__{entry['id_tema']}"
            data = {"id_realizacao": id_realizacao, "id_tema": entry["id_tema"]}
            batch.set(db.collection("realizacao_tema").document(id_document), data)
            batch_len += 1

            if batch_len >= 450:  # 500 is the limit for batch writes, we'll use 450 to be safe
                batch.commit()
                batch = db.batch()
                batch_len = 0

    if batch_len > 0:
        batch.commit()
