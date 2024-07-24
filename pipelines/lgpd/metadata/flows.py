# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.constants import constants
from pipelines.lgpd.metadata.schedules import update_metadata_schedule
from pipelines.lgpd.metadata.tasks import get_metadata_from_api
from pipelines.lgpd.tables_bindings.tasks import upload_dataframe_to_bigquery

with Flow(
    name="Gestão Datalake - Adquire metadados de tabelas",
    state_handlers=[handler_inject_bd_credentials],
    skip_if_running=True,
    parallelism=10,
) as rj_escritorio__lgpd__metadata__flow:
    # Parameters
    api_url = Parameter("api_url")
    dataset_id = Parameter("dataset_id")
    dump_mode = Parameter("dump_mode", default="append")
    table_id = Parameter("table_id")

    # Flow
    df_metadata = get_metadata_from_api(url=api_url)
    upload_task = upload_dataframe_to_bigquery(
        dataframe=df_metadata,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
    )


rj_escritorio__lgpd__metadata__flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio__lgpd__metadata__flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
rj_escritorio__lgpd__metadata__flow.schedule = update_metadata_schedule
