# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

every_minute_but_ten_times = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=1),
            start_date=datetime(2021, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
            labels=["escritoriodedados"],
            parameter_defaults={
                "a": "3",
                "b": str(i + 5),
            },
        )
        for i in range(10)
    ]
)
