# -*- coding: utf-8 -*-
"""
Flow definition for flooding detection using AI.
"""
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

from pipelines.constants import constants
from pipelines.deteccao_alagamento_cameras.flooding_detection.schedules import (
    update_flooding_data_schedule,
)

from pipelines.deteccao_alagamento_cameras.flooding_detection.tasks import (
    get_last_update,
    get_api_key,
    get_prediction,
    get_snapshot,
    pick_cameras,
    update_flooding_api_data,
)
from prefeitura_rio.pipelines_utils.custom import Flow

# Flow

with Flow(
    name="EMD: flooding_detection - Atualizar detecção de alagamento (IA) na API",
    code_owners=[
        "gabriel",
        "diego",
    ],
    skip_if_running=True,
) as rj_escritorio__flooding_detection__flow:
    # Parameters
    cameras_geodf_url = Parameter(
        "cameras_geodf_url",
        required=True,
        default="https://docs.google.com/spreadsheets/d/122uOaPr8YdW5PTzrxSPF-FD0tgco596HqgB7WK7cHFw/edit#gid=1580662721",
    )
    mocked_cameras_number = Parameter(
        "mocked_cameras_number",
        default=0,
    )
    openai_api_max_tokens = Parameter("openai_api_max_tokens", default=300)
    openai_api_model = Parameter("openai_api_model", default="gpt-4-vision-preview")
    openai_api_url = Parameter(
        "openai_api_url",
        default="https://api.openai.com/v1/chat/completions",
    )
    api_key_secret_path = Parameter("api_key_secret_path", required=True)
    rain_api_data_url = Parameter(
        "rain_api_url",
        default="https://api.dados.rio/v2/clima_pluviometro/precipitacao_15min/",
    )
    rain_api_update_url = Parameter(
        "rain_api_update_url",
        default="https://api.dados.rio/v2/clima_pluviometro/ultima_atualizacao_precipitacao_15min/",
    )
    redis_key_predictions_buffer = Parameter(
        "redis_key_predictions_buffer", default="flooding_detection_predictions_buffer"
    )
    redis_key_flooding_detection_data = Parameter(
        "redis_key_flooding_detection_data", default="flooding_detection_data"
    )
    redis_key_flooding_detection_last_update = Parameter(
        "redis_key_flooding_detection_last_update",
        default="flooding_detection_last_update",
    )

    # Flow
    last_update = get_last_update(rain_api_update_url=rain_api_update_url)
    cameras = pick_cameras(
        rain_api_data_url=rain_api_data_url,
        cameras_data_url=cameras_geodf_url,
        last_update=last_update,
        predictions_buffer_key=redis_key_predictions_buffer,
        number_mock_rain_cameras=mocked_cameras_number,
    )
    api_key = get_api_key(secret_path=api_key_secret_path, secret_name="GEMINI-PRO-VISION-API-KEY")
    cameras_with_image = get_snapshot.map(
        camera=cameras,
    )
    cameras_with_image_and_classification = get_prediction.map(
        camera_with_image=cameras_with_image,
        openai_api_model=unmapped(openai_api_model),
        api_key=unmapped(api_key),
        openai_api_max_tokens=unmapped(openai_api_max_tokens),
        openai_api_url=unmapped(openai_api_url),
    )
    update_flooding_api_data(
        cameras_with_image_and_classification=cameras_with_image_and_classification,
        data_key=redis_key_flooding_detection_data,
        last_update_key=redis_key_flooding_detection_last_update,
        predictions_buffer_key=redis_key_predictions_buffer,
    )


rj_escritorio__flooding_detection__flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio__flooding_detection__flow.executor = LocalDaskExecutor(num_workers=10)
rj_escritorio__flooding_detection__flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
rj_escritorio__flooding_detection__flow.schedule = update_flooding_data_schedule
