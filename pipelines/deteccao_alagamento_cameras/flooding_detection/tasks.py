# -*- coding: utf-8 -*-
import base64
import io
import json
import random
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Dict
from typing import List
from typing import Union

import cv2
import geopandas as gpd
import google.generativeai as genai
import numpy as np
import pandas as pd
import pendulum
import requests
from PIL import Image
from prefect import task
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client
from shapely.geometry import Point

from pipelines.deteccao_alagamento_cameras.flooding_detection.utils import download_file
from pipelines.deteccao_alagamento_cameras.flooding_detection.utils import redis_add_to_prediction_buffer
from pipelines.deteccao_alagamento_cameras.flooding_detection.utils import redis_get_prediction_buffer
# get_vault_secret


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
    object_prompt: str,
    google_api_key: str,
    google_api_model: str,
    google_api_max_output_tokens: int,
    google_api_temperature: float,
    google_api_top_p: int,
    google_api_top_k: int
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
            }
        flooding_prompt: The flooding prompt.
        google_api_key: The Google API key.
        google_api_model: The Google Gemini API model.
        google_api_max_tokens: The Google Gemini API max output tokens.
        google_api_temperature: The Google Gemini API temperature.
        google_api_top_p: The Google Gemini API top p.
        google_api_top_k: The Google Gemini API top k.

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
                }
            ],
        }
    """
    # TODO:
    # - Add confidence value
    # Setup the request
    if not camera_with_image["attempt_classification"]:
        camera_with_image["ai_classification"] = [
            {
                "object": "alagamento",
                "label": False,
                "confidence": 0.7,
            }
        ]
        return camera_with_image
    if not camera_with_image["image_base64"]:
        camera_with_image["ai_classification"] = [
            {
                "object": "alagamento",
                "label": None,
                "confidence": 0.7,
            }
        ]
        return camera_with_image

    flooding_detected = None

    genai.configure(api_key=google_api_key)
    model = genai.GenerativeModel(google_api_model)
    responses = model.generate_content(
        contents=[object_prompt, camera_with_image["image_base64"]],
        generation_config={
            "max_output_tokens": google_api_max_output_tokens,
            "temperature": google_api_temperature,
            "top_p": google_api_top_p,
            "top_k": google_api_top_k
        },
        stream=True,
    )

    responses.resolve()

    json_string = responses.text.replace("```json\n", "").replace("\n```", "")
    flooding_detected = json.loads(json_string)["flooding_detected"]

    log(f"Successfully got prediction: {flooding_detected}")

    camera_with_image["ai_classification"] = [
        {
            "object": "alagamento",
            "label": flooding_detected,
            "confidence": 0.7,
        }
    ]

    return camera_with_image


@task(
    max_retries=2,
    retry_delay=timedelta(seconds=1),
)
def get_snapshot(
    camera: Dict[str, Union[str, float]],
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
                "attempt_classification": True,
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
    try:
        rtsp_url = camera["url_camera"]
        cap = cv2.VideoCapture(rtsp_url)
        ret, frame = cap.read()
        if not ret:
            raise RuntimeError(f"Failed to get snapshot from URL {rtsp_url}.")
        cap.release()
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        img = Image.fromarray(frame)
        buffer = io.BytesIO()
        img.save(buffer, format="JPEG")
        img_b64 = base64.b64encode(buffer.getvalue()).decode("utf-8")
        log(f"Successfully got snapshot from URL {rtsp_url}.")
        camera["image_base64"] = img_b64
    except Exception:
        log(f"Failed to get snapshot from URL {rtsp_url}.")
        camera["image_base64"] = None
    return camera


@task
def pick_cameras(
    rain_api_data_url: str,
    cameras_data_url: str,
    last_update: datetime,
    predictions_buffer_key: str,
    number_mock_rain_cameras: int = 0,
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
                    "identifier": "alagamento"
                },
                ...
            ]
    """
    # Download the cameras data
    cameras_data_path = Path("/tmp") / "cameras_geo_min.csv"
    if not download_file(url=cameras_data_url, output_path=cameras_data_path):
        raise RuntimeError("Failed to download the cameras data.")

    cameras = pd.read_csv(cameras_data_path)

    # get only selected cameras from google sheets
    cameras = cameras[cameras["identificador"].notna()]

    cameras = cameras.drop(columns=["geometry"])
    geometry = [Point(xy) for xy in zip(cameras["longitude"], cameras["latitude"])]
    df_cameras = gpd.GeoDataFrame(cameras, geometry=geometry)
    df_cameras.crs = {"init": "epsg:4326"}
    log("Successfully downloaded cameras data.")
    log(f"Cameras shape: {df_cameras.shape}")

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

    # Modify status based on buffers
    for _, row in df_cameras_h3.iterrows():
        predictions_buffer_camera_key = f"{predictions_buffer_key}_{row['id_camera']}"
        predictions_buffer = redis_get_prediction_buffer(predictions_buffer_camera_key)
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

    # Set output
    output = []
    for _, row in df_cameras_h3.iterrows():
        output.append(
            {
                "id_camera": row["id_camera"],
                "nome_camera": row["nome"],
                "url_camera": row["rtsp"],
                "latitude": row["geometry"].y,
                "longitude": row["geometry"].x,
                "attempt_classification": (row["status"] not in ["sem chuva", "chuva fraca"]),
                "identifier": row["identificador"],
            }
        )
    log(f"Picked cameras: {output}")
    return output


@task
def update_flooding_api_data(
    cameras_with_image_and_classification: List[Dict[str, Union[str, float, bool]]],
    data_key: str,
    last_update_key: str,
    predictions_buffer_key: str,
) -> None:
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
        # Get AI classifications
        ai_classification = []
        current_prediction = camera_with_image_and_classification["ai_classification"][0]["label"]
        if current_prediction is None:
            api_data.append(
                {
                    "datetime": last_update.to_datetime_string(),
                    "id_camera": camera_with_image_and_classification["id_camera"],
                    "url_camera": camera_with_image_and_classification["url_camera"],
                    "latitude": camera_with_image_and_classification["latitude"],
                    "longitude": camera_with_image_and_classification["longitude"],
                    "image_base64": camera_with_image_and_classification["image_base64"],
                    "ai_classification": ai_classification,
                }
            )
            continue
        predictions_buffer_camera_key = (
            f"{predictions_buffer_key}_{camera_with_image_and_classification['id_camera']}"  # noqa
        )
        predictions_buffer = redis_add_to_prediction_buffer(
            predictions_buffer_camera_key, current_prediction
        )
        # Get most common prediction
        most_common_prediction = max(set(predictions_buffer), key=predictions_buffer.count)
        # Add classifications
        ai_classification.append(
            {
                "object": "alagamento",
                "label": most_common_prediction,
                "confidence": 0.7,
            }
        )
        api_data.append(
            {
                "datetime": last_update.to_datetime_string(),
                "id_camera": camera_with_image_and_classification["id_camera"],
                "url_camera": camera_with_image_and_classification["url_camera"],
                "latitude": camera_with_image_and_classification["latitude"],
                "longitude": camera_with_image_and_classification["longitude"],
                "image_base64": camera_with_image_and_classification["image_base64"],
                "ai_classification": ai_classification,
            }
        )

    # Update API data
    redis_client = get_redis_client(db=1)
    redis_client.set(data_key, api_data)
    redis_client.set(last_update_key, last_update.to_datetime_string())
    log("Successfully updated flooding detection data.")


@task
def get_object_parameters(object_spreadsheet_url: str) -> Dict[str, Dict[str, str]]:
    """
    Gets object parameters

    Args:
        object_spreadsheet_url: The object spreadsheet url

    Returns: The object parameters
    """
    parameters_data_path = Path("/tmp/object_parameters.csv")
    if not download_file(url=object_spreadsheet_url, output_path=parameters_data_path):
        raise RuntimeError("Failed to download the object parameters data.")

    parameters = pd.read_csv(parameters_data_path, index_col=0).to_dict(orient='index')

    return parameters
