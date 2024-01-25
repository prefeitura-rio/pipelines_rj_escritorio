# -*- coding: utf-8 -*-
"""
Database dumping flows.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from prefeitura_rio.pipelines_templates.dump_datario.flows import flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.dump_datario.dados_mestres_dump_datario.schedules import (
    dados_mestresmonthly_update_schedule,
)

dump_dados_mestres_flow = deepcopy(flow)
dump_dados_mestres_flow.name = "EMD: dados_mestres - Ingerir tabelas do data.rio"
dump_dados_mestres_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_dados_mestres_flow.state_handlers = [handler_inject_bd_credentials]
dump_dados_mestres_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
dump_dados_mestres_flow.schedule = dados_mestresmonthly_update_schedule
