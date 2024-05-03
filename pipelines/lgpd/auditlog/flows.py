# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.constants import constants
from pipelines.lgpd.auditlog.tasks import (
    get_auditlog_dataframe,
    get_last_execution_datetime,
    get_now,
    get_redis_url,
    set_last_execution_datetime,
)
from pipelines.lgpd.tables_bindings.tasks import (
    list_projects,
    merge_dataframes,
    upload_dataframe_to_bigquery,
)

with Flow(
    name="LGPD - Histórico de concessão de permissões no IAM",
    state_handlers=[handler_inject_bd_credentials],
    skip_if_running=True,
    parallelism=10,
) as rj_escritorio__lgpd__auditlog__flow:
    # Parameters
    credentials_secret_name = Parameter("credentials_secret_name")
    dataset_id = Parameter("dataset_id")
    dump_mode = Parameter("dump_mode", default="append")
    last_execution_redis_key = Parameter("last_execution_redis_key")
    redis_url_secret_name = Parameter("redis_url_secret_name")
    table_id = Parameter("table_id")

    # Flow
    now = get_now()
    redis_url = get_redis_url(secret_name=redis_url_secret_name)
    last_execution = get_last_execution_datetime(redis_url=redis_url, key=last_execution_redis_key)
    project_ids = list_projects(credentials_secret_name=credentials_secret_name)
    audit_log_dataframes = get_auditlog_dataframe.map(
        project_id=project_ids,
        credentials_secret_name=unmapped(credentials_secret_name),
        start=unmapped(last_execution),
        end=unmapped(now),
    )
    merged_dataframe = merge_dataframes(dfs=audit_log_dataframes)
    upload_task = upload_dataframe_to_bigquery(
        dataframe=merged_dataframe,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
    )
    set_last_execution_task = set_last_execution_datetime(
        redis_url=redis_url, key=last_execution_redis_key, value=now
    )
    set_last_execution_task.set_upstream(upload_task)


rj_escritorio__lgpd__auditlog__flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio__lgpd__auditlog__flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
# TODO: set schedule
# rj_escritorio__lgpd__auditlog__flow.schedule = update_tables_bindings_schedule
