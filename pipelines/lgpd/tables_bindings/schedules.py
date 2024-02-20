# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

update_tables_bindings_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2023, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
            labels=[
                constants.RJ_ESCRITORIO_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "credentials_secret_name": "LGPD_SERVICE_ACCOUNT_B64",
                "sheet_name": "tables_bindings",
                "spreadsheet_url": "https://docs.google.com/spreadsheets/d/16gVrhfwMl1TUZ_jbWdNKw1xcNMUTNwtkJCfg7nW52go/edit#gid=0",  # noqa
            },
        ),
    ]
)
