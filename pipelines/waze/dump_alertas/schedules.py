# -*- coding: utf-8 -*-
"""
Schedules for emd
"""

from datetime import datetime, timedelta

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

every_five_minutes = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.RJ_ESCRITORIO_AGENT_LABEL.value,
            ],
        ),
    ]
)
