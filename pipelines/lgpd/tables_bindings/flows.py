# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.constants import constants
from pipelines.lgpd.tables_bindings.schedules import update_tables_bindings_schedule
from pipelines.lgpd.tables_bindings.tasks import (
    get_project_tables_iam_policies,
    list_projects,
    merge_dataframes,
    upload_dataframe_to_bigquery,
)

with Flow(
    name="LGPD - Lista de permissões de acesso a tabelas do BigQuery",
    state_handlers=[handler_inject_bd_credentials],
    skip_if_running=True,
    parallelism=5,
) as rj_escritorio__lgpd__tables_bindings__flow:
    # Parameters
    credentials_secret_name = Parameter("credentials_secret_name")
    dataset_id = Parameter("dataset_id")
    dump_mode = Parameter("dump_mode", default="append")
    table_id = Parameter("table_id")

    # Flow
    project_ids = list_projects(credentials_secret_name=credentials_secret_name)
    iam_policies_dataframes = get_project_tables_iam_policies.map(
        project_id=project_ids, credentials_secret_name=unmapped(credentials_secret_name)
    )
    merged_dataframe = merge_dataframes(dfs=iam_policies_dataframes)
    upload_dataframe_to_bigquery(
        dataframe=merged_dataframe,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
    )


rj_escritorio__lgpd__tables_bindings__flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio__lgpd__tables_bindings__flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
rj_escritorio__lgpd__tables_bindings__flow.schedule = update_tables_bindings_schedule
