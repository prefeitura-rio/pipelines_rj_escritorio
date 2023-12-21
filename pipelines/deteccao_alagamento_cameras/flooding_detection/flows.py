# -*- coding: utf-8 -*-
"""
Flow definition for flooding detection using AI.
"""
from datetime import timedelta

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import task_get_current_flow_run_labels
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials
from prefeitura_rio.pipelines_utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_project_name,
)

from pipelines.constants import constants
from pipelines.deteccao_alagamento_cameras.flooding_detection.schedules import (
    update_flooding_data_schedule,
)
from pipelines.deteccao_alagamento_cameras.flooding_detection.tasks import (
    api_data_to_csv,
    get_api_key,
    get_last_update,
    get_prediction,
    get_snapshot,
    pick_cameras,
    task_get_redis_client,
    update_flooding_api_data,
)

with Flow(
    name="EMD: flooding_detection - Atualizar detecção de alagamento (IA) na API",
    state_handlers=[handler_inject_bd_credentials],
    skip_if_running=True,
    parallelism=30,
) as rj_escritorio__flooding_detection__flow:
    # Parameters
    cameras_geodf_url = Parameter(
        "cameras_geodf_url",
        required=True,
        default="https://docs.google.com/spreadsheets/d/122uOaPr8YdW5PTzrxSPF-FD0tgco596HqgB7WK7cHFw/edit#gid=914166579",  # noqa
    )
    mocked_cameras_number = Parameter(
        "mocked_cameras_number",
        default=0,
    )
    google_api_model = Parameter("google_api_model", default="gemini-pro-vision")
    api_key_secret_path = Parameter(
        "api_key_secret_path", required=True, default="/flooding-detection"
    )
    object_parameters_url = Parameter(
        "object_parameters_url",
        required=True,
        default="https://docs.google.com/spreadsheets/d/122uOaPr8YdW5PTzrxSPF-FD0tgco596HqgB7WK7cHFw/edit#gid=1580662721",  # noqa
    )
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
    dataset_id = Parameter("dataset_id", default="ai_vision_detection")
    table_id = Parameter("table_id", default="cameras_predicoes")
    materialize_after_dump = Parameter("materialize_after_dump", default=False)

    current_flow_labels = task_get_current_flow_run_labels()

    # Flow
    redis_client = task_get_redis_client(
        infisical_host_env="REDIS_HOST",
        infisical_port_env="REDIS_PORT",
        infisical_db_env="REDIS_DB",
        infisical_password_env="REDIS_PASSWORD",
        infisical_secrets_path=api_key_secret_path,
    )
    last_update = get_last_update(rain_api_update_url=rain_api_update_url)
    cameras = pick_cameras(
        rain_api_data_url=rain_api_data_url,
        cameras_data_url=cameras_geodf_url,
        object_parameters_url=object_parameters_url,
        last_update=last_update,
        predictions_buffer_key=redis_key_predictions_buffer,
        redis_client=redis_client,
        number_mock_rain_cameras=mocked_cameras_number,
    )
    api_key = get_api_key(secret_path=api_key_secret_path, secret_name="GEMINI-PRO-VISION-API-KEY")
    cameras_with_image = get_snapshot.map(
        camera=cameras,
    )

    cameras_with_image_and_classification = get_prediction.map(
        camera_with_image=cameras_with_image,
        google_api_key=unmapped(api_key),
        google_api_model=unmapped(google_api_model),
    )

    api_data, has_api_data = update_flooding_api_data(
        cameras_with_image_and_classification=cameras_with_image_and_classification,
        data_key=redis_key_flooding_detection_data,
        last_update_key=redis_key_flooding_detection_last_update,
        predictions_buffer_key=redis_key_predictions_buffer,
        redis_client=redis_client,
    )

    with case(has_api_data, True):
        data_path = api_data_to_csv(
            data_path="/tmp/api_data_cameras/", api_data=api_data, api_model=google_api_model
        )

        create_staging_table = create_table_and_upload_to_gcs(
            data_path=data_path,
            dataset_id=dataset_id,
            table_id=table_id,
            biglake_table=True,
            dump_mode="append",
        )

        with case(materialize_after_dump, True):
            materialization_flow = create_flow_run(
                flow_name=settings.FLOW_NAME_EXECUTE_DBT_MODEL,
                project_name=get_current_flow_project_name(),
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "mode": "dev",
                    "materialize_to_datario": False,
                    "dbt_model_secret_parameters": {},
                    "dbt_alias": False,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id}",
            )
            materialization_flow.set_upstream(create_staging_table)
            wait_for_materialization = wait_for_flow_run(
                materialization_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )
            wait_for_materialization.max_retries = settings.TASK_MAX_RETRIES_DEFAULT
            wait_for_materialization.retry_delay = timedelta(
                seconds=settings.TASK_RETRY_DELAY_DEFAULT
            )

rj_escritorio__flooding_detection__flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio__flooding_detection__flow.executor = LocalDaskExecutor(num_workers=10)
rj_escritorio__flooding_detection__flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
rj_escritorio__flooding_detection__flow.schedule = update_flooding_data_schedule
