# -*- coding: utf-8 -*-
import base64
import collections
import json
import traceback
from datetime import timedelta
from typing import Any, Dict, List, Tuple

import firebase_admin
import pandas as pd
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
    remove_double_spaces,
    to_snake_case,
)
from pipelines.utils import authenticated_task as task


@task(nout=5, checkpoint=False)
def cleanup_unused(
    cidades: List[dict],
    orgaos: List[dict],
    programas: List[dict],
    realizacoes: List[dict],
    statuses: List[dict],
    temas: List[dict],
) -> None:
    """
    Cleanup unused stuff.
    """
    # Iterate over realizacoes and count the number of times each id appears
    id_cidade = {}
    id_orgao = {}
    id_programa = {}
    id_status = {}
    id_tema = {}
    for realizacao in realizacoes:
        data = realizacao["data"]
        if data["id_cidade"] not in id_cidade:
            id_cidade[data["id_cidade"]] = []
        id_cidade[data["id_cidade"]].append(realizacao["id"])
        if data["id_orgao"] not in id_orgao:
            id_orgao[data["id_orgao"]] = []
        id_orgao[data["id_orgao"]].append(realizacao["id"])
        if data["id_programa"] not in id_programa:
            id_programa[data["id_programa"]] = []
        id_programa[data["id_programa"]].append(realizacao["id"])
        if data["id_status"] not in id_status:
            id_status[data["id_status"]] = []
        id_status[data["id_status"]].append(realizacao["id"])
        if data["id_tema"] not in id_tema:
            id_tema[data["id_tema"]] = []
        id_tema[data["id_tema"]].append(realizacao["id"])

    # Collect the IDs from the original lists
    possible_id_cidades = [cidade["id"] for cidade in cidades]
    possible_id_orgaos = [orgao["id"] for orgao in orgaos]
    possible_id_programas = [programa["id"] for programa in programas]
    possible_id_statuses = [status["id"] for status in statuses]
    possible_id_temas = [tema["id"] for tema in temas]

    # Show the IDs that do not exist in the original lists
    for id_ in id_cidade:
        if id_ not in possible_id_cidades:
            log(
                f"ID {id_} does not exist in the cidades list. Occurrences: {id_cidade[id_]}.",
                "warning",
            )
    for id_ in id_orgao:
        if id_ not in possible_id_orgaos:
            log(
                f"ID {id_} does not exist in the orgaos list. Occurrences: {id_orgao[id_]}.",
                "warning",
            )
    for id_ in id_programa:
        if id_ not in possible_id_programas:
            log(
                f"ID {id_} does not exist in the programas list. Occurrences: {id_programa[id_]}.",
                "warning",
            )
    for id_ in id_status:
        if id_ not in possible_id_statuses:
            log(
                f"ID {id_} does not exist in the statuses list. Occurrences: {id_status[id_]}.",
                "warning",
            )
    for id_ in id_tema:
        if id_ not in possible_id_temas:
            log(
                f"ID {id_} does not exist in the temas list. Occurrences: {id_tema[id_]}.",
                "warning",
            )

    # Show the IDs that are not being used and remove them from the original lists
    clean_cidades = []
    for cidade in cidades:
        if cidade["id"] not in id_cidade:
            log(f"Cidade {cidade['id']} is not being used in the realizacoes list.", "warning")
        else:
            clean_cidades.append(cidade)
    clean_orgaos = []
    for orgao in orgaos:
        if orgao["id"] not in id_orgao:
            log(f"Órgão {orgao['id']} is not being used in the realizacoes list.", "warning")
        else:
            clean_orgaos.append(orgao)
    clean_programas = []
    for programa in programas:
        if programa["id"] not in id_programa:
            log(f"Programa {programa['id']} is not being used in the realizacoes list.", "warning")
        else:
            clean_programas.append(programa)
    clean_statuses = []
    for status in statuses:
        if status["id"] not in id_status:
            log(f"Status {status['id']} is not being used in the realizacoes list.", "warning")
        else:
            clean_statuses.append(status)
    clean_temas = []
    for tema in temas:
        if tema["id"] not in id_tema:
            log(f"Tema {tema['id']} is not being used in the realizacoes list.", "warning")
        else:
            clean_temas.append(tema)

    # Return the cleaned lists
    return clean_cidades, clean_orgaos, clean_programas, clean_statuses, clean_temas


@task
def compute_aggregate_data(realizacoes: list[dict]):
    """
    Pre-computes aggregated data for Firebase.
    """
    aggregated_data = collections.defaultdict(lambda: {"count": 0, "investment": 0})
    for realizacao in realizacoes:
        data = realizacao["data"]
        filters = [
            ("bairro", data["id_bairro"] if data["id_bairro"] else "none"),
            ("programa", data["id_programa"] if data["id_programa"] else "none"),
            ("subprefeitura", data["id_subprefeitura"] if data["id_subprefeitura"] else "none"),
            ("tema", data["id_tema"] if data["id_tema"] else "none"),
        ]

        # Generate all combinations of filters
        log(f"Filters: {filters}")
        for i in range(16):  # 2^4 for 4 filters (on/off)
            keys = []
            for j in range(4):
                if i & (1 << j):
                    keys.append("__".join(filters[j]))
            key = "___".join(keys) if keys else "all"
            aggregated_data[key]["count"] += 1
            aggregated_data[key]["investment"] += data["investimento"]

    # Convert defaultdict to a regular dictionary for JSON serialization
    aggregated_data = {str(k): v for k, v in aggregated_data.items()}
    return aggregated_data


@task(checkpoint=False)
def filter_out_nones(data: list) -> list:
    return [entry for entry in data if entry is not None]


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
        entry["nome_extenso"] = (
            entry["nome_extenso"].replace("/", "")
            if entry["nome_extenso"]
            else (entry["nome"].replace("/", "") if entry["nome"] else "")
        )
        data.append(
            {
                "id": to_snake_case(entry["nome_extenso"]),
                "data": {
                    "id_cidade": "rio_de_janeiro",
                    "nome": remove_double_spaces(entry["nome_extenso"]),
                    "sigla": remove_double_spaces(entry["sigla"]) if entry["sigla"] else None,
                },
            }
        )
    return data


@task(checkpoint=False)
def get_infopref_programa(url_programa: str, headers: dict) -> list[dict]:
    raw_data = fetch_data(url_programa, headers)
    data = []
    for entry in raw_data:
        entry["programa"] = remove_double_spaces(entry["programa"].replace("/", ""))
        entry["descricao"] = remove_double_spaces(
            entry["descricao"].replace("\n", "\\n").replace("\r", "\\r")
        )
        entry["tema"] = remove_double_spaces(entry["tema"].replace("/", ""))
        data.append(
            {
                "id": to_snake_case(entry["programa"]),
                "data": {
                    "descricao": entry["descricao"],
                    "id_tema": to_snake_case(entry["tema"]),
                    "image_url": entry["imagem_url"],
                    "nome": entry["programa"],
                },
            }
        )
    return data


@task(checkpoint=False)
def get_infopref_realizacao_raw(url_realizacao: str, headers: dict) -> list[dict]:
    realizacoes = fetch_data(url_realizacao, headers)
    return realizacoes


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
        {
            "id": "parado",
            "data": {
                "nome": "Parado",
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
        entry["nome"] = remove_double_spaces(entry["tema"].replace("/", ""))
        entry["descricao"] = remove_double_spaces(
            entry["descricao"].replace("\n", "\\n").replace("\r", "\\r")
        )
        data.append(
            {
                "id": to_snake_case(entry["nome"]),
                "data": {
                    "descricao": entry["descricao"],
                    "image_url": entry["imagem_url"],
                    "nome": entry["nome"],
                },
            }
        )
    return data


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
def merge_lists(list_a: list, list_b: list) -> list:
    return list_a + list_b


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


@task
def log_task(msg: str) -> None:
    log(msg)


@task(nout=2, checkpoint=False)
def split_by_gestao(
    realizacoes: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Split the realizacoes by gestao.

    Args:
        realizacoes (List[Dict[str, Any]]): The realizacoes.

    Returns:
        Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]: The realizacoes split by gestao.
    """
    new = []
    old = []
    for realizacao in realizacoes:
        if realizacao["data"]["gestao"] == "3":
            new.append(realizacao)
        else:
            old.append(realizacao)
    return new, old


@task(checkpoint=False)
def transform_csv_to_pin_only_realizacoes(
    csv_url: str,
    id_tema: str,
    id_programa: str,
    gestao: str,
    bairros: List[Dict[str, Any]],
    force_pass: bool = False,
) -> List[Dict[str, Any]]:
    """
    Transforms the CSV data to the pin-only realizacoes format.
    CSV must have three columns: "nome", "latitude" and "longitude".

    Args:
        csv_url (str): The URL of the CSV file.

    Returns:
        List[Dict[str, Any]]: The pin-only realizacoes.
    """

    def parse_single_row(row: pd.Series) -> Dict[str, Any]:
        try:
            latitude = row["latitude"]
            if isinstance(latitude, str):
                latitude = latitude.replace(",", ".")
            latitude = float(latitude)
            longitude = row["longitude"]
            if isinstance(longitude, str):
                longitude = longitude.replace(",", ".")
            longitude = float(longitude)
            coords = GeoPoint(latitude, longitude)
            bairro = get_bairro_from_lat_long(coords.latitude, coords.longitude, bairros)
            id_bairro = to_snake_case(bairro["nome"])
            nome = remove_double_spaces(" ".join(row["nome"].split()).strip())
            nome = nome.replace("/", "")
            data = {
                "cariocas_atendidos": None,
                "coords": coords,
                "data_fim": None,
                "data_inicio": None,
                "descricao": None,
                "destaque": False,
                "endereco": None,
                "gestao": gestao,
                "id_bairro": id_bairro,
                "id_cidade": "rio_de_janeiro",
                "id_orgao": None,
                "id_programa": id_programa,
                "id_status": "concluído",
                "id_subprefeitura": None,
                "id_tema": id_tema,
                "id_tipo": "obra",
                "image_folder": None,  # TODO: deprecate this field
                "image_url": None,
                "investimento": None,
                "nome": nome,
            }
            return {"id": to_snake_case(row["nome"]), "data": data}
        except Exception as exc:
            log("Could not parse row.")
            log(f"Raw row: {row}")
            log(traceback.format_exc())
            if force_pass:
                return None
            raise exc

    df = pd.read_csv(csv_url)
    try:
        assert "nome" in df.columns
        assert "latitude" in df.columns
        assert "longitude" in df.columns
    except AssertionError:
        raise ValueError("CSV must have 'nome', 'latitude' and 'longitude' columns.")

    ret = [parse_single_row(row) for _, row in df.iterrows()]
    ret = [entry for entry in ret if entry is not None]
    return ret


@task(
    max_retries=3,
    retry_delay=timedelta(seconds=5),
)
def transform_infopref_realizacao_to_firebase(
    entry: Dict[str, Any],
    gmaps_key: str,
    db: FirestoreClient,
    bairros: List[Dict[str, Any]],
    temas: List[dict],
    force_pass: bool = False,
) -> Dict[str, Any]:
    """
    Transform the infopref data to the firebase format.

    Args:
        entry (Dict[str, Any]): The entry.

    Returns:
        Dict[str, Any]: The transformed entry.
    """
    try:
        log(f"Raw entry: {entry}")
        # Get title first
        nome = remove_double_spaces(" ".join(entry["titulo"].split()).strip())
        nome = nome.replace("/", "")
        # Transform fields
        cariocas_atendidos = (
            int(entry["populacao_beneficiada"]) if entry["populacao_beneficiada"] else 0
        )
        latitude: float = None
        longitude: float = None
        if nome == "Estação Lobo Junior":
            latitude = -22.829339
            longitude = -43.271627
        elif nome == "Estação Baixa do Sapateiro":
            latitude = -22.859869
            longitude = -43.247791
        elif entry["lat"] and entry["lng"]:
            latitude = float(entry["lat"].replace(",", "."))
            if latitude <= -90 or latitude >= 90:
                latitude = None
            longitude = float(entry["lng"].replace(",", "."))
            if longitude <= -180 or longitude >= 180:
                longitude = None
        if latitude is None or longitude is None:
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
        descricao = (
            remove_double_spaces(
                entry["descricao_projeto"].replace("\n", "\\n").replace("\r", "\\r")
            )
            if entry["descricao_projeto"]
            else None
        )
        destaque = entry["destaque"] == "sim"
        endereco = remove_double_spaces(entry["logradouro"]) if entry["logradouro"] else None
        gestao = entry["gestao"]
        id_bairro = to_snake_case(entry["bairro"])
        id_status = to_snake_case(entry["status"])
        image_url = entry["imagem_url"]
        investimento = float(entry["investimento"]) if entry["investimento"] else 0
        id_orgao = to_snake_case(
            remove_double_spaces(entry["orgao_extenso"]) if entry["orgao_extenso"] else ""
        )
        id_programa = to_snake_case(
            remove_double_spaces(entry["programa"]) if entry["programa"] else ""
        )
        id_tema = to_snake_case(remove_double_spaces(entry["tema"]) if entry["tema"] else "")
        # Get fields from related collections
        # - id_subprefeitura: in the "bairro" collection, find the document that matches the
        #   id_bairro and get the id_subprefeitura field
        # - id_cidade: in the "subprefeitura" collection, find the document that matches the
        #   id_subprefeitura and get the id_cidade field
        all_id_bairros = [doc.id for doc in db.collection("bairro").stream()]
        id_subprefeitura: str = None
        if id_bairro not in all_id_bairros:
            try:
                bairro = get_bairro_from_lat_long(coords.latitude, coords.longitude, bairros)
                id_bairro = to_snake_case(bairro["nome"])
                if id_bairro not in all_id_bairros:
                    raise ValueError(f"Could not find bairro with id {id_bairro}.")
                entry["id_bairro"] = id_bairro
            except ValueError:
                id_bairro = None
                log(
                    (
                        f"Could not find bairro for ({coords.latitude},{coords.longitude})."
                        f"\nRaw data: {entry}"
                    ),
                    "warning",
                )
        else:
            bairro_doc = db.collection("bairro").document(id_bairro).get()
            id_subprefeitura = bairro_doc.to_dict()["id_subprefeitura"]
            subprefeitura_doc = db.collection("subprefeitura").document(id_subprefeitura).get()
            if not subprefeitura_doc.exists:
                log(f"Could not find subprefeitura with id {id_subprefeitura}.", "warning")

        data = {
            "cariocas_atendidos": cariocas_atendidos,
            "coords": coords,
            "data_fim": data_fim,
            "data_inicio": data_inicio,
            "descricao": descricao,
            "destaque": destaque,
            "endereco": endereco,
            "gestao": gestao,
            "id_bairro": id_bairro,
            "id_cidade": "rio_de_janeiro",
            "id_orgao": id_orgao,
            "id_programa": id_programa,
            "id_status": id_status,
            "id_subprefeitura": id_subprefeitura,
            "id_tema": id_tema,
            "id_tipo": "obra",
            "image_folder": None,  # TODO: deprecate this field
            "image_url": image_url,
            "investimento": investimento,
            "nome": nome,
        }
        log(f"Transformed entry: {data}")
        return {"id": to_snake_case(nome), "data": data}
    except Exception as exc:
        log("Could not transform entry.")
        log(f"Raw entry: {entry}")
        log(traceback.format_exc())
        if force_pass:
            return None
        raise exc


@task
def upload_aggregated_data_to_firestore(
    data: dict,
    db: FirestoreClient,
    collection: str = "aggregated_data",
    clear: bool = True,
    n_splits: int = 5,
) -> None:
    """
    Upload the aggregated data to Firestore.
    """
    log("Uploading aggregated data to Firestore.")
    if clear:
        # Delete the collection
        docs = db.collection(collection).stream()
        batch = db.batch()
        batch_len = 0
        for doc in docs:
            batch.delete(doc.reference)
            batch_len += 1
            if batch_len >= 450:
                batch.commit()
                batch = db.batch()
                batch_len = 0
        if batch_len > 0:
            batch.commit()
    # Split the data into n_splits parts
    data_splits = []
    for i in range(n_splits):
        data_splits.append({k: v for j, (k, v) in enumerate(data.items()) if j % n_splits == i})
    batch = db.batch()
    batch_len = 0
    for i, data in enumerate(data_splits):
        doc_ref = db.collection(collection).document(f"summary_{i}")
        batch.set(doc_ref, data)
        batch_len += 1
        if batch_len >= 450:
            batch.commit()
            batch = db.batch()
            batch_len = 0
    if batch_len > 0:
        batch.commit()


@task
def upload_infopref_data_to_firestore(
    data: List[Dict[str, Any]], db: FirestoreClient, collection: str, clear: bool = True
) -> None:
    """
    Upload the infopref data to Firestore.

    Args:
        data (List[Dict[str, Any]]): The data.
    """
    log(f"Uploading {len(data)} documents to collection {collection}.")
    if clear:
        # Delete the collection
        docs = db.collection(collection).stream()
        batch = db.batch()
        batch_len = 0
        for doc in docs:
            batch.delete(doc.reference)
            batch_len += 1
            if batch_len >= 450:
                batch.commit()
                batch = db.batch()
                batch_len = 0
        if batch_len > 0:
            batch.commit()
    batch = db.batch()
    batch_len = 0
    unique_titles = {}
    for entry in data:
        # Add document to collection
        id_ = entry["id"]
        data = entry["data"]
        if collection == "realizacao":
            title = data["nome"]
            if title in unique_titles:
                log(f"Document {title} is duplicated. Renaming", "warning")
                unique_titles[title] += 1
                title = f"{title} ({unique_titles[title]})"
            else:
                unique_titles[title] = 1
            data["nome"] = title
            id_ = to_snake_case(title)
        try:
            batch.set(db.collection(collection).document(id_), data)
            batch_len += 1
        except ValueError as exc:
            log(f"Could not upload document {id_}. Reason: {exc}", "error")
            raise exc

        if batch_len >= 450:  # 500 is the limit for batch writes, we'll use 450 to be safe
            batch.commit()
            batch = db.batch()
            batch_len = 0

    if batch_len > 0:
        batch.commit()
