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
# SMI Dashboard de Obras Schedules
#
#####################################

identidade_unica_tables = {
    "identidade": "identidade",
    "interacao": "interacao",
}

identidade_unica_clocks = [
    IntervalClock(
        interval=timedelta(days=30),
        start_date=datetime(2023, 5, 20, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo"))
        + timedelta(minutes=2 * count),
        labels=[
            constants.RJ_ESCRITORIO_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": "identidade_unica",
            "table_id": table_id,
            "mode": "prod",
        },
    )
    for count, (_, table_id) in enumerate(identidade_unica_tables.items())
]
update_schedule = Schedule(clocks=untuple(identidade_unica_clocks))
