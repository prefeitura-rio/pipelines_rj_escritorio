# -*- coding: utf-8 -*-
"""
DBT-related flows.
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

from pipelines.constants import constants
from pipelines.identidade_unica.dbt_identidade_unica.schedules import (
    update_schedule,
)

run_dbt_identidade_unica_flow = deepcopy(templates__run_dbt_model__flow)
run_dbt_identidade_unica_flow.name = "Identidade Única: Materializar tabelas"
run_dbt_identidade_unica_flow.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]
run_dbt_identidade_unica_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_identidade_unica_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)

identidade_unica_default_parameters = {
    "dataset_id": "identidade_unica",
    "upstream": False,
    "dbt_alias": False,
}
run_dbt_identidade_unica_flow = set_default_parameters(
    run_dbt_identidade_unica_flow,
    default_parameters=identidade_unica_default_parameters,
)

run_dbt_identidade_unica_flow.schedule = update_schedule
