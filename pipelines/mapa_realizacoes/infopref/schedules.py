# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

every_day_at_3am = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 3, tzinfo=pytz.timezone("America/Sao_Paulo")),
            labels=["escritoriodedados"],
            parameter_defaults={
                "force_pass": True,
            },
        )
    ]
)
