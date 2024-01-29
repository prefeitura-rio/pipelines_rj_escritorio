# -*- coding: utf-8 -*-
"""
Dumping  data from INEA FTP to BigQuery
"""
# pylint: disable=E1101,C0103,bad-continuation

from copy import deepcopy

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials
from prefeitura_rio.pipelines_utils.tasks import get_on_redis, save_on_redis

from pipelines.constants import constants
from pipelines.dump_ftp.inea.schedules import (
    every_1_day,
    every_1_day_mac,
    every_5_minutes,
    every_5_minutes_mac,
)
from pipelines.dump_ftp.inea.tasks import (
    download_files,
    get_files_datalake,
    get_files_from_ftp,
    get_ftp_client,
    select_files_to_download,
    upload_file_to_gcs,
)

with Flow(
    name="INEA: Captura FTP dados de radar (Guaratiba)",
    state_handlers=[handler_inject_bd_credentials],
) as inea_ftp_radar_flow:
    bucket_name = Parameter("bucket_name", default="rj-escritorio-dev", required=False)
    date = Parameter("date", default=None, required=False)
    get_only_last_file = Parameter("get_only_last_file", default=True, required=False)
    greater_than = Parameter("greater_than", default=None, required=False)
    check_datalake_files = Parameter("check_datalake_files", default=True, required=False)
    prefix = Parameter("prefix", default="raw/meio_ambiente_clima/inea_radar_hdf5", required=False)
    mode = Parameter("mode", default="prod", required=False)
    radar = Parameter("radar", default="gua", required=False)
    product = Parameter("product", default="ppi", required=False)
    api_key_secret_path = Parameter("api_key_secret_path", required=True, default="/dump_ftp")

    client = get_ftp_client(secret_path=api_key_secret_path)

    files = get_files_from_ftp(
        client=client,
        radar=radar,
    )

    redis_files = get_on_redis(
        dataset_id="meio_ambiente_clima",
        table_id=radar,
        mode=mode,
        wait=files,
    )

    datalake_files = get_files_datalake(
        bucket_name=bucket_name,
        prefix=prefix,
        radar=radar,
        product=product,
        date=date,
        greater_than=greater_than,
        check_datalake_files=check_datalake_files,
        mode=mode,
        wait=files,
    )

    files_to_download = select_files_to_download(
        files=files,
        redis_files=redis_files,
        datalake_files=datalake_files,
        date=date,
        greater_than=greater_than,
        get_only_last_file=get_only_last_file,
    )

    files_to_upload = download_files(client=client, files=files_to_download, radar=radar)

    upload_files = upload_file_to_gcs.map(
        file_to_upload=files_to_upload,
        bucket_name=unmapped(bucket_name),
        prefix=unmapped(prefix),
        mode=unmapped(mode),
        radar=unmapped(radar),
        product=unmapped(product),
    )

    save_on_redis(
        dataset_id="meio_ambiente_clima",
        table_id=radar,
        mode=mode,
        files=files_to_upload,
        keep_last=14400,  # last 30 days files
        wait=upload_files,
    )

inea_ftp_radar_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inea_ftp_radar_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
inea_ftp_radar_flow.schedule = every_5_minutes

inea_ftp_radar_flow_mac = deepcopy(inea_ftp_radar_flow)
inea_ftp_radar_flow_mac.name = "INEA: Captura FTP dados de radar (Macaé)"
inea_ftp_radar_flow_mac.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inea_ftp_radar_flow_mac.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
inea_ftp_radar_flow_mac.schedule = every_5_minutes_mac

inea_ftp_radar_flow_fill_missing = deepcopy(inea_ftp_radar_flow)
inea_ftp_radar_flow_fill_missing.name = (
    "INEA: Captura FTP dados de radar (Guaratiba): preenchimento de arquivos faltantes"
)
inea_ftp_radar_flow_fill_missing.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inea_ftp_radar_flow_fill_missing.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
inea_ftp_radar_flow_fill_missing.schedule = every_1_day

inea_ftp_radar_flow_fill_missing_mac = deepcopy(inea_ftp_radar_flow)
inea_ftp_radar_flow_fill_missing_mac.name = (
    "INEA: Captura FTP dados de radar (Macaé): preenchimento de arquivos faltantes"
)
inea_ftp_radar_flow_fill_missing_mac.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inea_ftp_radar_flow_fill_missing_mac.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
inea_ftp_radar_flow_fill_missing_mac.schedule = every_1_day_mac

inea_ftp_backfill_radar_flow = deepcopy(inea_ftp_radar_flow)
inea_ftp_backfill_radar_flow.name = "INEA: Captura dados de radar (backfill)"
inea_ftp_backfill_radar_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inea_ftp_backfill_radar_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
inea_ftp_backfill_radar_flow.schedule = None
