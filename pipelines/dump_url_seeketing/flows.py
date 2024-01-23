# -*- coding: utf-8 -*-
"""
Database dumping flows for Setur project
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.dump_url_seeketing.schedules import (
    gsheets_daily_update_schedule,
)
from prefeitura_rio.pipelines_templates.dump_url.flows import dump_url_flow
from prefeitura_rio.pipelines_utils.prefect import set_default_parameters

seeketing_gsheets_flow = deepcopy(dump_url_flow)
seeketing_gsheets_flow.name = "SETUR: Seeketing - Ingerir tabelas de URL"
seeketing_gsheets_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
seeketing_gsheets_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
)

setur_gsheets_default_parameters = {}
seeketing_gsheets_flow = set_default_parameters(
    seeketing_gsheets_flow, default_parameters=setur_gsheets_default_parameters
)

seeketing_gsheets_flow.schedule = gsheets_daily_update_schedule
