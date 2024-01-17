# -*- coding: utf-8 -*-
"""
Flow definition for flooding detection using AI.
"""
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials
from prefeitura_rio.pipelines_utils.tasks import create_table_and_upload_to_gcs

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
    upload_image_to_gcs,
    upload_to_native_table,
)

with Flow(
    name="EMD: flooding_detection - Atualizar detecção de alagamento (IA) na API",
    state_handlers=[handler_inject_bd_credentials],
    skip_if_running=True,
    parallelism=100,
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
    use_rain_api_data = Parameter(
        "use_rain_api_data",
        default=True,
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
    resize_width = Parameter("resize_width", default=640)
    resize_height = Parameter("resize_height", default=480)
    snapshot_timeout = Parameter("snapshot_timeout", default=300)

    image_upload_bucket = Parameter(
        "image_upload_bucket",
        default="datario-public",
    )
    image_upload_blob_prefix = Parameter(
        "image_upload_blob_prefix",
        default="flooding_detection/latest_snapshots",
    )
    dataset_id = Parameter("dataset_id", default="ai_vision_detection")
    table_id = Parameter("table_id", default="cameras_predicoes")

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
        use_rain_api_data=use_rain_api_data,
    )
    api_key = get_api_key(secret_path=api_key_secret_path, secret_name="GEMINI-PRO-VISION-API-KEY")
    cameras_with_image = get_snapshot.map(
        camera=cameras,
        resize_width=unmapped(resize_width),
        resize_height=unmapped(resize_height),
        snapshot_timeout=unmapped(snapshot_timeout),
    )

    cameras_with_image_url = upload_image_to_gcs.map(
        camera_with_image=cameras_with_image,
        bucket_name=unmapped(image_upload_bucket),
        blob_base_path=unmapped(image_upload_blob_prefix),
    )

    cameras_with_image_and_classification = get_prediction.map(
        camera_with_image=cameras_with_image_url,
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
        data_path, dataframe = api_data_to_csv(
            data_path="/tmp/api_data_cameras/", api_data=api_data, api_model=google_api_model
        )

        create_staging_table = create_table_and_upload_to_gcs(
            data_path=data_path,
            dataset_id=dataset_id,
            table_id=table_id,
            biglake_table=True,
            dump_mode="append",
        )
        create_staging_table.set_upstream(data_path)

        update_native_table = upload_to_native_table(
            dataset_id=dataset_id, table_id=table_id, dataframe=dataframe
        )
        update_native_table.set_upstream(create_staging_table)


rj_escritorio__flooding_detection__flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio__flooding_detection__flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
rj_escritorio__flooding_detection__flow.schedule = update_flooding_data_schedule
