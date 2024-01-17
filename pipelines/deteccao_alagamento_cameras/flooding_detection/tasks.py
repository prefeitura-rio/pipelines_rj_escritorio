# -*- coding: utf-8 -*-
import base64
import io
import json
import random
import time
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Union
from uuid import uuid4

import basedosdados as bd
import cv2
import geopandas as gpd
import google.generativeai as genai
import pandas as pd
import pendulum
import requests
from google.cloud import bigquery
from PIL import Image
from prefect import task
from prefeitura_rio.pipelines_utils.gcs import upload_file_to_bucket
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.io import to_partitions
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.pandas import parse_date_columns
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client
from prefeitura_rio.pipelines_utils.time import TimeoutError
from redis_pal import RedisPal
from shapely.geometry import Point

from pipelines.deteccao_alagamento_cameras.flooding_detection.utils import (
    download_file,
    get_video_capture,
    redis_add_to_prediction_buffer,
    redis_get_prediction_buffer,
)


@task(checkpoint=False)
def task_get_redis_client(
    infisical_host_env: str = "REDIS_HOST",
    infisical_port_env: str = "REDIS_PORT",
    infisical_db_env: str = "REDIS_DB",
    infisical_password_env: str = "REDIS_PASSWORD",
    infisical_secrets_path: str = "/",
):
    """
    Gets a Redis client.

    Args:
        infisical_host_env: The environment variable for the Redis host.
        infisical_port_env: The environment variable for the Redis port.
        infisical_db_env: The environment variable for the Redis database.
        infisical_password_env: The environment variable for the Redis password.

    Returns:
        The Redis client.
    """
    redis_host = get_secret(infisical_host_env, path=infisical_secrets_path)[infisical_host_env]
    redis_port = int(
        get_secret(infisical_port_env, path=infisical_secrets_path)[infisical_port_env]
    )
    redis_db = int(get_secret(infisical_db_env, path=infisical_secrets_path)[infisical_db_env])
    redis_password = get_secret(infisical_password_env, path=infisical_secrets_path)[
        infisical_password_env
    ]
    return get_redis_client(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        password=redis_password,
    )


@task
def get_last_update(
    rain_api_update_url: str,
) -> datetime:
    """
    Gets the last update datetime from the rain API.

    Args:
        rain_api_update_url: The rain API update url.

    Returns:
        The last update datetime.
    """
    data = requests.get(rain_api_update_url).text
    data = data.strip('"')
    log(f"Last update: {data}")
    return datetime.strptime(data, "%d/%m/%Y %H:%M:%S")


@task
def get_api_key(secret_path: str, secret_name: str = "GEMINI-PRO-VISION-API-KEY") -> str:
    """
    Gets the GEMINI API KEY.

    Args:
        secret_path: The secret path.
        secret_name: The secret name.

    Returns:
        The API key.
    """

    secret = get_secret(secret_name=secret_name, path=secret_path)
    return secret[secret_name]


@task(
    max_retries=3,
    retry_delay=timedelta(seconds=1),
)
def get_prediction(
    camera_with_image: Dict[str, Union[str, float]],
    google_api_key: str,
    google_api_model: str,
) -> Dict[str, Union[str, float, bool]]:
    """
    Gets the flooding detection prediction from Google Gemini API.

    Args:
        camera_with_image: The camera with image in the following format:
            {
                "id_camera": "1",
                "url_camera": "rtsp://...",
                "latitude": -22.912,
                "longitude": -43.230,
                "image_base64": "base64...",
                "attempt_classification": True,
                "object": "alagamento",
                "prompt": "You are ....",
                "max_output_token": 300,
                "temperature": 0.4,
                "top_k": 1,
                "top_p": 32,
            }
        google_api_key: The Google API key.

    Returns: The camera with image and classification in the following format:
        {
            "id_camera": "1",
            "url_camera": "rtsp://...",
            "latitude": -22.912,
            "longitude": -43.230,
            "image_base64": "base64...",
            "ai_classification": [
                {
                    "object": "alagamento",
                    "label": True,
                    "confidence": 0.7,
                    "prompt": "You are ....",
                    "max_output_token": 300,
                    "temperature": 0.4,
                    "top_k": 1,
                    "top_p": 32,
                }
            ],
        }
    """
    # TODO:
    # - Add confidence value
    # Setup the request
    log(f"Getting prediction for id_camera: {camera_with_image['id_camera']}")  # noqa
    log(f"Getting prediction for object: {camera_with_image['object']}")  # noqa
    log(
        f"Getting prediction for camera_with_image: {camera_with_image['image_base64'][:20] + '...' if camera_with_image['image_base64'] else None}"  # noqa
    )
    if not camera_with_image["attempt_classification"]:
        log("Skipping prediction for `attempt_classification` is False.")
        camera_with_image["ai_classification"] = [
            {
                "object": camera_with_image["object"],
                "label": False,
                "confidence": 0.7,
                "prompt": camera_with_image["prompt"],
                "max_output_token": camera_with_image["max_output_token"],
                "temperature": camera_with_image["temperature"],
                "top_k": camera_with_image["top_k"],
                "top_p": camera_with_image["top_p"],
            }
        ]
        return camera_with_image
    if not camera_with_image["image_base64"]:
        log("Skipping prediction for `image_base64` is None.")
        camera_with_image["ai_classification"] = [
            {
                "object": camera_with_image["object"],
                "label": None,
                "confidence": 0.7,
                "prompt": camera_with_image["prompt"],
                "max_output_token": camera_with_image["max_output_token"],
                "temperature": camera_with_image["temperature"],
                "top_k": camera_with_image["top_k"],
                "top_p": camera_with_image["top_p"],
            }
        ]
        return camera_with_image

    label = None

    img = Image.open(io.BytesIO(base64.b64decode(camera_with_image["image_base64"])))
    genai.configure(api_key=google_api_key)
    model = genai.GenerativeModel(google_api_model)
    responses = model.generate_content(
        contents=[camera_with_image["prompt"], img],
        generation_config={
            "max_output_tokens": camera_with_image["max_output_token"],
            "temperature": camera_with_image["temperature"],
            "top_p": camera_with_image["top_p"],
            "top_k": camera_with_image["top_k"],
        },
        stream=True,
    )

    responses.resolve()
    if type(responses) == tuple:
        responses = responses[0]
    json_string = responses.text.replace("```json\n", "").replace("\n```", "")
    label = json.loads(json_string)["label"]

    log(f"Successfully got prediction: {label}")

    camera_with_image["ai_classification"] = [
        {
            "object": camera_with_image["object"],
            "label": label,
            "confidence": 0.7,
            "prompt": camera_with_image["prompt"],
            "max_output_token": camera_with_image["max_output_token"],
            "temperature": camera_with_image["temperature"],
            "top_k": camera_with_image["top_k"],
            "top_p": camera_with_image["top_p"],
        }
    ]

    return camera_with_image


@task(
    max_retries=2,
    retry_delay=timedelta(seconds=1),
)
def get_snapshot(
    camera: Dict[str, Union[str, float]],
    resize_width: int = 640,
    resize_height: int = 480,
    snapshot_timeout: int = 300,
) -> Dict[str, Union[str, float]]:
    """
    Gets a snapshot from a camera.

    Args:
        camera: The camera in the following format:
            {
                "id_camera": "1",
                "url_camera": "rtsp://...",
                "latitude": -22.912,
                "longitude": -43.230,
                "image_base64": "base64...",
                "attempt_classification": True,
                "object": "alagamento",
                "prompt": "You are ....",
                "max_output_token": 300,
                "temperature": 0.4,
                "top_k": 1,
                "top_p": 32,

            }

    Returns:
        The camera with image in the following format:
            {
                "id_camera": "1",
                "url_camera": "rtsp://...",
                "latitude": -22.912,
                "longitude": -43.230,
                "attempt_classification": True,
                "image_base64": "base64...",
            }
    """
    camera_id = camera.get("id_camera")
    object_name = camera.get("object")
    rtsp_url = camera.get("url_camera")

    camera_log = f"camera_id: {camera_id}\nobject: {object_name}\n"
    try:
        start_time = time.time()
        cap, ret, frame = get_video_capture(rtsp_url=rtsp_url, timeout=snapshot_timeout)
        if not ret:
            raise RuntimeError("No ret returned.")
        cap.release()
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        img = Image.fromarray(frame)
        img.thumbnail((resize_width, resize_height))
        buffer = io.BytesIO()
        img.save(buffer, format="JPEG")
        img_b64 = base64.b64encode(buffer.getvalue()).decode("utf-8")

        log(
            msg=f"Successfully got snapshot from URL {rtsp_url}.\n{camera_log}\nTake {round(time.time() - start_time, 3)} seconds."  # noqa
        )
        camera["image_base64"] = img_b64
    except TimeoutError as e:
        log(
            msg=f"Timeout to get snapshot from URL {rtsp_url}.\n{camera_log}\nTake {round(time.time() - start_time, 3)} seconds.\n\nError:\n\n{e}",  # noqa
            level="warning",
        )
        camera["image_base64"] = None

    except Exception as e:
        log(
            f"Failed to get snapshot from URL {rtsp_url}.\n{camera_log}\nTake {round(time.time() - start_time, 3)} seconds.\n\nError:\n\n{e}",  # noqa
            level="warning",
        )
        camera["image_base64"] = None

    return camera


@task
def pick_cameras(
    rain_api_data_url: str,
    cameras_data_url: str,
    object_parameters_url: str,
    last_update: datetime,
    predictions_buffer_key: str,
    redis_client: RedisPal,
    number_mock_rain_cameras: int = 0,
    use_rain_api_data: bool = True,
) -> List[Dict[str, Union[str, float]]]:
    """
    Picks cameras based on the raining hexagons and last update.

    Args:
        rain_api_data_url: The rain API data url.
        last_update: The last update datetime.
        predictions_buffer_key: The Redis key for the predictions buffer.

    Returns:
        A list of cameras in the following format:
            [
                {
                    "id_camera": "1",
                    "url_camera": "rtsp://...",
                    "latitude": -22.912,
                    "longitude": -43.230,
                    "attempt_classification": True,
                    "object": "alagamento",
                    "prompt": "You are ....",
                    "max_output_token": 300,
                    "temperature": 0.4,
                    "top_k": 1,
                    "top_p": 32,
                },
                ...
            ]
    """
    # Download the cameras data
    cameras_data_path = Path("/tmp") / "cameras_geo_min.csv"
    if not download_file(url=cameras_data_url, output_path=cameras_data_path):
        raise RuntimeError("Failed to download the cameras data.")

    cameras = pd.read_csv(cameras_data_path)
    cameras["id_camera"] = cameras["id_camera"].astype(str).str.zfill(6)
    # get only selected cameras from google sheets
    cameras = cameras[cameras["identificador"].notna()]

    cameras = cameras.drop(columns=["geometry"])
    geometry = [Point(xy) for xy in zip(cameras["longitude"], cameras["latitude"])]
    df_cameras = gpd.GeoDataFrame(cameras, geometry=geometry)
    df_cameras.crs = {"init": "epsg:4326"}
    log("Successfully downloaded cameras data.")
    log(f"Cameras shape: {df_cameras.shape}")

    if use_rain_api_data:
        # Get rain data
        rain_data = requests.get(rain_api_data_url).json()
        df_rain = pd.DataFrame(rain_data)
        df_rain["last_update"] = last_update
        log("Successfully downloaded rain data.")
        log(f"Rain data shape: {df_rain.shape}")

        # Join the dataframes
        df_cameras_h3 = pd.merge(df_cameras, df_rain, how="left", on="id_h3")
        log("Successfully joined the dataframes.")
        log(f"Cameras H3 shape: {df_cameras_h3.shape}")
    else:
        df_cameras_h3 = df_cameras.copy()
        df_cameras_h3["status"] = None

    # Modify status based on buffers
    for _, row in df_cameras_h3.iterrows():
        predictions_buffer_camera_key = f"{predictions_buffer_key}_{row['id_camera']}"
        predictions_buffer = redis_get_prediction_buffer(
            predictions_buffer_camera_key, redis_client=redis_client
        )
        # Get most common prediction
        most_common_prediction = max(set(predictions_buffer), key=predictions_buffer.count)
        # Get last prediction
        last_prediction = predictions_buffer[-1]
        # Add classifications
        if most_common_prediction or last_prediction:
            row["status"] = "chuva moderada"

    # Mock a few cameras when argument is set
    if number_mock_rain_cameras > 0:
        df_len = len(df_cameras_h3)
        for _ in range(number_mock_rain_cameras):
            mocked_index = random.randint(0, df_len)
            df_cameras_h3.loc[mocked_index, "status"] = "chuva moderada"
            log(f'Mocked camera ID: {df_cameras_h3.loc[mocked_index]["id_camera"]}')

    # expand dataframe when have multiples objects
    df_cameras_h3_expanded = pd.DataFrame()
    for _, row in df_cameras_h3.iterrows():
        objetos = row["identificador"].split(",")
        for objeto in objetos:
            row["identificador"] = objeto.strip()
            df_cameras_h3_expanded = pd.concat([df_cameras_h3_expanded, pd.DataFrame([row])])

    # download the object parameters data
    parameters_data_path = Path("/tmp/object_parameters.csv")
    if not download_file(url=object_parameters_url, output_path=parameters_data_path):
        raise RuntimeError("Failed to download the object parameters data.")
    parameters = pd.read_csv(parameters_data_path)

    # add the parameters to the cameras
    df_cameras_h3_expanded = df_cameras_h3_expanded.merge(
        parameters, left_on="identificador", right_on="objeto", how="left"
    )
    # Set output
    output = []
    for _, row in df_cameras_h3_expanded.iterrows():
        output.append(
            {
                "id_camera": row["id_camera"],
                "nome_camera": row["nome"],
                "url_camera": row["rtsp"],
                "latitude": row["geometry"].y,
                "longitude": row["geometry"].x,
                "attempt_classification": True,  # noqa (row["status"] not in ["sem chuva", "chuva fraca"]),
                "object": row["identificador"],
                "prompt": row["prompt"],
                "max_output_token": row["max_output_token"],
                "temperature": row["temperature"],
                "top_k": row["top_k"],
                "top_p": row["top_p"],
            }
        )

    output_log = json.dumps(output, indent=4)
    log(f"Picked cameras:\n {output_log}")
    return output


@task(nout=2)
def update_flooding_api_data(
    cameras_with_image_and_classification: List[Dict[str, Union[str, float, bool]]],
    data_key: str,
    last_update_key: str,
    predictions_buffer_key: str,
    redis_client: RedisPal,
) -> Tuple[List[Dict[str, Union[str, float, bool]]], bool]:
    """
    Updates Redis keys with flooding detection data and last update datetime (now).

    Args:
        cameras_with_image_and_classification: The cameras with image and classification
            in the following format:
                [
                    {
                        "id_camera": "1",
                        "url_camera": "rtsp://...",
                        "latitude": -22.912,
                        "longitude": -43.230,
                        "image_base64": "base64...",
                        "ai_classification": [
                            {
                                "object": "alagamento",
                                "label": True,
                                "confidence": 0.7,
                                "prompt": "You are ....",
                                "max_output_token": 300,
                                "temperature": 0.4,
                                "top_k": 1,
                                "top_p": 32,
                            }
                        ],
                    },
                    ...
                ]
        data_key: The Redis key for the flooding detection data.
        last_update_key: The Redis key for the last update datetime.
        predictions_buffer_key: The Redis key for the predictions buffer.
    """
    # Build API data
    last_update = pendulum.now(tz="America/Sao_Paulo")
    api_data = []
    for camera_with_image_and_classification in cameras_with_image_and_classification:
        ai_classification_api_list = []
        for ai_classification in camera_with_image_and_classification["ai_classification"]:
            # Get AI classifications
            current_prediction = ai_classification[0]["label"]
            if current_prediction is None:
                ai_classification_api_list.append(
                    {
                        "object": camera_with_image_and_classification["object"],
                        "label": None,
                        "confidence": None,
                        "prompt": camera_with_image_and_classification["prompt"],
                        "max_output_token": camera_with_image_and_classification[
                            "max_output_token"
                        ],
                        "temperature": camera_with_image_and_classification["temperature"],
                        "top_k": camera_with_image_and_classification["top_k"],
                        "top_p": camera_with_image_and_classification["top_p"],
                    }
                )
            else:
                # TODO: add object the key name. Currently working for just one object
                predictions_buffer_camera_key = f"{predictions_buffer_key}_{camera_with_image_and_classification['id_camera']}"  # noqa
                predictions_buffer = redis_add_to_prediction_buffer(
                    predictions_buffer_camera_key, current_prediction, redis_client=redis_client
                )
                # Get most common prediction
                most_common_prediction = max(set(predictions_buffer), key=predictions_buffer.count)

                ai_classification_api_list.append(
                    {
                        "object": camera_with_image_and_classification["object"],
                        "label": most_common_prediction,
                        "confidence": 0.7,
                        "prompt": camera_with_image_and_classification["prompt"],
                        "max_output_token": camera_with_image_and_classification[
                            "max_output_token"
                        ],
                        "temperature": camera_with_image_and_classification["temperature"],
                        "top_k": camera_with_image_and_classification["top_k"],
                        "top_p": camera_with_image_and_classification["top_p"],
                    }
                )

        # Add classifications
        api_data_dict = {
            "datetime": last_update.to_datetime_string(),
            "id_camera": camera_with_image_and_classification["id_camera"],
            "url_camera": camera_with_image_and_classification["url_camera"],
            "latitude": camera_with_image_and_classification["latitude"],
            "longitude": camera_with_image_and_classification["longitude"],
            "image_base64": camera_with_image_and_classification["image_base64"],
            "image_url": camera_with_image_and_classification["image_url"],
            "ai_classification": ai_classification_api_list,
        }
        api_data.append(api_data_dict)

    bq_data = deepcopy(api_data)
    # clean api_data
    for d in api_data:
        d.pop("image_base64", None)
        for c in d["ai_classification"]:
            c.pop("prompt", None)
            c.pop("max_output_token", None)
            c.pop("temperature", None)
            c.pop("top_k", None)
            c.pop("top_p", None)

    # Update API data
    redis_client.set(data_key, api_data)
    redis_client.set(last_update_key, last_update.to_datetime_string())
    log("Successfully updated flooding detection data.")

    has_api_data = not len(api_data) == 0
    log(f"has_api_data: {has_api_data}")

    return bq_data, has_api_data


@task(nout=2)
def api_data_to_csv(
    data_path: str | Path, api_data: List[Dict[str, Union[str, float, bool]]], api_model: str
) -> Tuple[str | Path, pd.DataFrame]:
    base_path = Path(data_path)

    data_normalized = []
    for d in api_data:
        normalized_dict = {}
        for k, v in d.items():
            if k == "ai_classification":
                if len(v) > 0:
                    normalized_dict = normalized_dict | v[0]
            else:
                normalized_dict[k] = v
        data_normalized.append(normalized_dict)
    dataframe = pd.DataFrame.from_records(data_normalized)
    dataframe["model"] = api_model
    dataframe, partition_columns = parse_date_columns(
        dataframe=dataframe, partition_date_column="datetime"
    )
    saved_files = to_partitions(
        data=dataframe,
        partition_columns=partition_columns,
        savepath=base_path,
        data_type="csv",
        suffix=f"{datetime.now().strftime('%Y%m%d-%H%M%S')}",
    )
    log(f"saved_files:{saved_files}")
    return base_path, dataframe


@task
def upload_to_native_table(
    dataset_id: str, table_id: str, dataframe: pd.DataFrame, wait=None
) -> None:
    """
    Upload data to native table.
    """
    table = bd.Table(dataset_id=dataset_id, table_id=table_id)

    # create some columns and cast type
    dataframe["datetime"] = pd.to_datetime(dataframe["datetime"])
    dataframe["data_particao"] = dataframe["datetime"].apply(lambda x: str(x)[:10])
    dataframe["data_particao"] = pd.to_datetime(dataframe["data_particao"])

    dataframe["geometry"] = (
        "POINT ("
        + dataframe["longitude"].astype(str)
        + " "
        + dataframe["latitude"].astype(str)
        + ")"
    )
    dataframe["id_camera"] = dataframe["id_camera"].astype(str).str.zfill(6)

    schema = [
        bigquery.SchemaField("data_particao", "DATE"),
        bigquery.SchemaField("datetime", "DATETIME"),
        bigquery.SchemaField("id_camera", "STRING"),
        bigquery.SchemaField("url_camera", "STRING"),
        bigquery.SchemaField("model", "STRING"),
        bigquery.SchemaField("object", "STRING"),
        bigquery.SchemaField("label", "BOOL"),
        bigquery.SchemaField("confidence", "FLOAT64"),
        bigquery.SchemaField("prompt", "STRING"),
        bigquery.SchemaField("max_output_token", "INT64"),
        bigquery.SchemaField("temperature", "FLOAT64"),
        bigquery.SchemaField("top_k", "INT64"),
        bigquery.SchemaField("top_p", "INT64"),
        bigquery.SchemaField("latitude", "FLOAT64"),
        bigquery.SchemaField("longitude", "FLOAT64"),
        bigquery.SchemaField("geometry", "GEOGRAPHY"),
        bigquery.SchemaField("image_base64", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition="WRITE_APPEND",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_particao",  # name of column to use for partitioning
        ),
    )

    col_order = [col.name for col in schema]
    dataframe = dataframe[col_order]
    cols = dataframe.columns.tolist()
    shape = dataframe.shape
    log(f"Write dataframe shape: {shape}")
    log(f"Write dataframe columns: {cols}")

    job = table.client["bigquery_prod"].load_table_from_dataframe(
        dataframe, table.table_full_name["prod"], job_config=job_config
    )

    job.result()


@task
def upload_image_to_gcs(
    camera_with_image: Dict[str, Union[str, float]], bucket_name: str, blob_base_path: str
) -> Dict[str, Union[str, float]]:
    """
    Uploads an image to GCS.

    Args:
        camera_with_image: The camera with image in the following format:
            {
                "id_camera": "1",
                "url_camera": "rtsp://...",
                "latitude": -22.912,
                "longitude": -43.230,
                "image_base64": "base64...",
                "attempt_classification": True,
                "object": "alagamento",
                "prompt": "You are ....",
                "max_output_token": 300,
                "temperature": 0.4,
                "top_k": 1,
                "top_p": 32,
            }
        bucket_name: The GCS bucket name.
        blob_base_path: The GCS blob base path.

    Returns:
        The camera with image in the following format:
            {
                "id_camera": "1",
                "url_camera": "rtsp://...",
                "latitude": -22.912,
                "longitude": -43.230,
                "image_base64": "base64...",
                "attempt_classification": True,
                "object": "alagamento",
                "prompt": "You are ....",
                "max_output_token": 300,
                "temperature": 0.4,
                "top_k": 1,
                "top_p": 32,
                "image_url": "https://storage.googleapis.com/...",
            }
    """
    if not camera_with_image["image_base64"]:
        log("Skipping upload for `image_base64` is None.")
        camera_with_image["image_url"] = None
        return camera_with_image
    try:
        # Remove trailing slash
        blob_base_path = blob_base_path.rstrip("/")
        # Save image to temp file
        tmp_fname = f"/tmp/{uuid4()}.png"
        img = Image.open(io.BytesIO(base64.b64decode(camera_with_image["image_base64"])))
        with open(tmp_fname, "wb") as f:
            img.save(f, format="PNG")
        # Set blob path
        camera_id = camera_with_image["id_camera"]
        blob_path = f"{blob_base_path}/{camera_id}.png"
        log(f"Uploading image to GCS: {blob_path}")
        blob = upload_file_to_bucket(
            bucket_name=bucket_name,
            file_path=tmp_fname,
            destination_blob_name=blob_path,
        )
        image_url = blob.public_url
        camera_with_image["image_url"] = image_url
        log(f"Successfully uploaded image to GCS: {blob_path}")
    except Exception:
        log(f"Failed to upload image to GCS: {blob_path}")
        camera_with_image["image_url"] = None
    return camera_with_image
