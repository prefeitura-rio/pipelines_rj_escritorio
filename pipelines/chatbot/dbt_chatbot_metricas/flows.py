# -*- coding: utf-8 -*-
"""
DBT-related flows.......
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_templates.run_dbt_model.flows import (
    templates__run_dbt_model__flow,
)
from prefeitura_rio.pipelines_utils.prefect import set_default_parameters
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.chatbot.dbt_chatbot_metricas.schedules import update_schedule
from pipelines.constants import constants

run_rbt_chatbot_flow = deepcopy(templates__run_dbt_model__flow)
run_rbt_chatbot_flow.name = "Chatbot: Materializar tabelas"
run_rbt_chatbot_flow.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]
run_rbt_chatbot_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_rbt_chatbot_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)

identidade_unica_default_parameters = {
    "dataset_id": "dialogflowcx",
    "upstream": False,
    "dbt_alias": False,
}
run_rbt_chatbot_flow = set_default_parameters(
    run_rbt_chatbot_flow,
    default_parameters=identidade_unica_default_parameters,
)

run_rbt_chatbot_flow.schedule = update_schedule

# comment to trigger a new build again and again and again and again
