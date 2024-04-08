# -*- coding: utf-8 -*-
import base64
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

from pipelines.mapa_realizacoes.infopref.utils import to_camel_case, to_snake_case
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


@task(
    max_retries=3,
    retry_delay=timedelta(seconds=5),
)
def transform_infopref_to_firebase(entry: Dict[str, Any], gmaps_key: str) -> Dict[str, Any]:
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

    output = {
        "cariocas_atendidos": cariocas_atendidos,
        "coords": coords,
        "data_fim": data_fim,
        "data_inicio": data_inicio,
        "descricao": descricao,
        "id_bairro": id_bairro,
        "id_status": id_status,
        "id_tipo": "obra",
        "image_folder": image_folder,
        "investimento": investimento,
        "nome": nome,
        "id_orgao": id_orgao,
        "id_programa": id_programa,
        "id_tema": id_tema,
    }
    return output


@task
def upload_infopref_data_to_firestore(data: List[Dict[str, Any]]) -> None:
    """
    Upload the infopref data to Firestore.

    Args:
        data (List[Dict[str, Any]]): The data.
    """
    # Initialize Firebase client
    cred = credentials.Certificate("/tmp/firebase-credentials.json")
    app = firebase_admin.initialize_app(cred)
    db: FirestoreClient = firestore.client(app)

    # Get all id_bairro, id_status, id_orgao, id_programa, id_tema from Firestore
    all_id_bairros = [doc.id for doc in db.collection("bairro").stream()]
    all_id_status = [doc.id for doc in db.collection("status").stream()]
    all_id_orgaos = [doc.id for doc in db.collection("orgao").stream()]
    all_id_programas = [doc.id for doc in db.collection("programa").stream()]
    all_id_temas = [doc.id for doc in db.collection("tema").stream()]

    # Check if all IDs from the input data are present in Firestore
    batch = db.batch()
    batch_len = 0
    ok_entries = []
    for entry in data:
        if entry["id_bairro"] not in all_id_bairros:
            log(f"ID {entry['id_bairro']} not found in Firestore for realizacao {data}.", "warning")
            continue
        if entry["id_status"] not in all_id_status:
            log(f"ID {entry['id_status']} not found in Firestore for realizacao {data}.", "warning")
            continue
        if entry["id_orgao"] not in all_id_orgaos:
            log(f"ID {entry['id_orgao']} not found in Firestore for realizacao {data}.", "warning")
            continue
        if entry["id_programa"] not in all_id_programas:
            batch.set(
                db.collection("programa").document(entry["id_programa"]),
                {"nome": to_camel_case(entry["id_programa"]), "descricao": ""},
            )
            batch_len += 1
        if entry["id_tema"] not in all_id_temas:
            batch.set(
                db.collection("tema").document(entry["id_tema"]),
                {"nome": to_camel_case(entry["id_tema"])},
            )
            batch_len += 1
        ok_entries.append(entry)
        if batch_len > 0:
            batch.commit()
            batch = db.batch()
            batch_len = 0

    # Upload data to Firestore. This will be done in a few steps:
    # - First we have to add the document to the `realizacao` collection. This will have all the
    # fields from the input data except for `id_orgao`, `id_programa` and `id_tema`.
    # - Then we have to add the document to the `realizacao_orgao`, `realizacao_programa` and
    # `realizacao_tema` collections. These collections will have the `realizacao_id` and the
    # respective `id_orgao`, `id_programa` and `id_tema`.
    batch = db.batch()
    for entry in ok_entries:
        # Add document to `realizacao` collection
        id_realizacao = to_snake_case(entry["nome"])
        data = {
            "cariocas_atendidos": entry["cariocas_atendidos"],
            "coords": entry["coords"],
            "data_fim": entry["data_fim"],
            "data_inicio": entry["data_inicio"],
            "descricao": entry["descricao"],
            "id_bairro": entry["id_bairro"],
            "id_status": entry["id_status"],
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
        id_document = f"{id_realizacao}__{entry['id_orgao']}"
        data = {"id_realizacao": id_realizacao, "id_orgao": entry["id_orgao"]}
        batch.set(db.collection("realizacao_orgao").document(id_document), data)
        batch_len += 1

        if batch_len >= 450:  # 500 is the limit for batch writes, we'll use 450 to be safe
            batch.commit()
            batch = db.batch()
            batch_len = 0

        # Add document to `realizacao_programa` collection
        id_document = f"{id_realizacao}__{entry['id_programa']}"
        data = {"id_realizacao": id_realizacao, "id_programa": entry["id_programa"]}
        batch.set(db.collection("realizacao_programa").document(id_document), data)
        batch_len += 1

        if batch_len >= 450:  # 500 is the limit for batch writes, we'll use 450 to be safe
            batch.commit()
            batch = db.batch()
            batch_len = 0

        # Add document to `realizacao_tema` collection
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
