# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants

#####################################
#
# Chatbot Metrics Schedules
#
#####################################

chatbot_tables = {
    "fim_conversas": {
        "table_id": "fim_conversas",
        "upstream": True,
    },
    "historico_conversas_legivel": {
        "table_id": "historico_conversas_legivel",
        "upstream": False,
    },
    "conversas_completas_metricas": {
        "table_id": "conversas_completas_metricas",
        "upstream": False,
    },
}

chatbot_clocks = [
    IntervalClock(
        interval=timedelta(hours=12),
        start_date=datetime(2024, 9, 17, 19, 0, tzinfo=pytz.timezone("America/Sao_Paulo"))
        + timedelta(minutes=45 * count),
        labels=[
            constants.RJ_ESCRITORIO_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": "dialogflowcx",
            "table_id": parameters["table_id"],
            "upstream": parameters["upstream"],
            "infisical_credential_path": {
                "secret_path": "/dbt-rj-chatbot-dev",
                "secret_name": "SEVICE_ACCOUNT",
            },
            "dbt_project_materialization": "rj-chatbot-dev",
        },
    )
    for count, (_, parameters) in enumerate(chatbot_tables.items())
]
update_schedule = Schedule(clocks=untuple(chatbot_clocks))
