# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.constants import constants
from pipelines.lgpd.tables_bindings.schedules import update_tables_bindings_schedule
from pipelines.lgpd.tables_bindings.tasks import (
    get_project_tables_iam_policies,
    list_projects,
    merge_dataframes,
    upload_dataframe_to_gsheets,
)

with Flow(
    name="LGPD - Lista de permissões de acesso a tabelas do BigQuery",
    state_handlers=[handler_inject_bd_credentials],
    skip_if_running=True,
    parallelism=100,
) as rj_escritorio__lgpd__tables_bindings__flow:
    # Parameters
    credentials_secret_name = Parameter("credentials_secret_name")
    sheet_name = Parameter("sheet_name")
    spreadsheet_url = Parameter("spreadsheet_url")

    # Flow
    project_ids = list_projects(credentials_secret_name=credentials_secret_name)
    iam_policies_dataframes = get_project_tables_iam_policies.map(project_id=project_ids)
    merged_dataframe = merge_dataframes(dfs=iam_policies_dataframes)
    upload_dataframe_to_gsheets(
        dataframe=merged_dataframe,
        spreadsheet_url=spreadsheet_url,
        sheet_name=sheet_name,
        credentials_secret_name=credentials_secret_name,
    )


rj_escritorio__lgpd__tables_bindings__flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio__lgpd__tables_bindings__flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
rj_escritorio__lgpd__tables_bindings__flow.schedule = update_tables_bindings_schedule
