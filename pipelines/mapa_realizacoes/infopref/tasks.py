# -*- coding: utf-8 -*-
import base64
import json
from datetime import timedelta
from typing import Any, Dict, List

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore import GeoPoint
from google.cloud.firestore_v1.client import Client as FirestoreClient
from googlemaps import Client as GoogleMapsClient
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.logging import log
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon

from pipelines.mapa_realizacoes.infopref.utils import (
    fetch_data,
    get_bairro_from_lat_long,
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


@task(checkpoint=False)
def get_infopref_bairro() -> list[dict]:
    return []  # TODO: Implement (future)


@task(checkpoint=False)
def get_infopref_cidade() -> list[dict]:
    return [
        {
            "id": "rio_de_janeiro",
            "data": {
                "nome": "Rio de Janeiro",
            },
        }
    ]


@task(checkpoint=False)
def get_infopref_orgao(url_orgao: str, headers: dict) -> list[dict]:
    raw_data = fetch_data(url_orgao, headers)
    data = []
    for entry in raw_data:
        data.append(
            {
                "id": to_snake_case(entry["nome_extenso"]),
                "data": {
                    "id_cidade": "rio_de_janeiro",
                    "nome": entry["nome_extenso"],
                    "sigla": entry["sigla"],
                },
            }
        )
    return data


@task(checkpoint=False)
def get_infopref_programa(url_programa: str, headers: dict) -> list[dict]:
    raw_data = fetch_data(url_programa, headers)
    data = []
    for entry in raw_data:
        data.append(
            {
                "id": to_snake_case(entry["programa"]),
                "data": {
                    "descricao": entry["descricao"],
                    "id_tema": to_snake_case(entry["tema"]),
                    "nome": entry["programa"],
                },
            }
        )
    return data


@task(checkpoint=False)
def get_infopref_realizacao_raw(url_realizacao: str, headers: dict) -> list[dict]:
    return fetch_data(url_realizacao, headers)


@task(checkpoint=False)
def get_infopref_status() -> list[dict]:
    return [
        {
            "id": "concluído",
            "data": {
                "nome": "Concluído",
            },
        },
        {
            "id": "em_andamento",
            "data": {
                "nome": "Em andamento",
            },
        },
        {
            "id": "em_licitação",
            "data": {
                "nome": "Em licitação",
            },
        },
        {
            "id": "interrompida",
            "data": {
                "nome": "Interrompida",
            },
        },
    ]


@task(checkpoint=False)
def get_infopref_subprefeitura() -> list[dict]:
    return []  # TODO: Implement (future)


@task(checkpoint=False)
def get_infopref_tema(url_tema: str, headers: dict) -> list[dict]:
    raw_data = fetch_data(url_tema, headers)
    data = []
    for entry in raw_data:
        data.append(
            {
                "id": to_snake_case(entry["nome"]),
                "data": {
                    "descricao": entry["descricao"],
                    "nome": entry["nome"],
                },
            }
        )


@task(checkpoint=False)
def get_infopref_tipo() -> list[dict]:
    return []  # TODO: Implement (future)


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
def transform_infopref_realizacao_to_firebase(
    entry: Dict[str, Any], gmaps_key: str, db: FirestoreClient, bairros: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Transform the infopref data to the firebase format.

    Args:
        entry (Dict[str, Any]): The entry.

    Returns:
        Dict[str, Any]: The transformed entry.
    """
    # Transform fields
    cariocas_atendidos = (
        int(entry["populacao_beneficiada"]) if entry["populacao_beneficiada"] else 0
    )
    if entry["lat"] and entry["lng"]:
        latitude = float(entry["lat"])
        longitude = float(entry["lng"])
    else:
        gmaps_client: GoogleMapsClient = GoogleMapsClient(key=gmaps_key)
        full_address = f"{entry['logradouro']}, {entry['bairro']}, Rio de Janeiro, Brazil"
        geocode_result = gmaps_client.geocode(full_address)
        # If we fail to geo-locate the address, we use the neighborhood as a fallback
        if not geocode_result:
            log(
                f"Could not geocode address {full_address}. Falling back to neighborhood.",
                "warning",
            )
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

    data = {
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
    return {"id": to_snake_case(nome).replace("/", "-"), "data": data}


@task
def upload_infopref_data_to_firestore(
    data: List[Dict[str, Any]], db: FirestoreClient, collection: str, clear: bool = True
) -> None:
    """
    Upload the infopref data to Firestore.

    Args:
        data (List[Dict[str, Any]]): The data.
    """
    if clear:
        # Delete the collection
        db.collection(collection).delete()
    batch = db.batch()
    batch_len = 0
    for entry in data:
        # Add document to collection
        id_ = entry["id"]
        data = entry["data"]
        batch.set(db.collection(collection).document(id_), data)
        batch_len += 1

        if batch_len >= 450:  # 500 is the limit for batch writes, we'll use 450 to be safe
            batch.commit()
            batch = db.batch()
            batch_len = 0

    if batch_len > 0:
        batch.commit()
