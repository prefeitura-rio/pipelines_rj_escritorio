# -*- coding: utf-8 -*-
"""
MATERIALIZA MODELOS DO DBT.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_templates.dump_to_gcs.flows import (
    flow,
)
from prefeitura_rio.pipelines_utils.prefect import set_default_parameters
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.constants import constants

templates__run_dbt_model_emd__flow = deepcopy(flow)
templates__run_dbt_model_emd__flow.state_handlers = [handler_inject_bd_credentials]

templates__run_dbt_model_emd__flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
templates__run_dbt_model_emd__flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        #constants.RJ_ESCRITORIO_AGENT_LABEL.value,
        constants.RJ_DATARIO_AGENT_LABEL.value,
    ],
)

templates_run_dbt_model_emd_default_parameters = {
    "dataset_id": "dataset_id",
    "table_id": "table_id",
}
templates__run_dbt_model_emd__flow = set_default_parameters(
    templates__run_dbt_model_emd__flow,
    default_parameters=templates_run_dbt_model_emd_default_parameters,
)
