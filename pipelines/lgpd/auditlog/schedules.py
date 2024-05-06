# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

update_audit_logs_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2023, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
            labels=[
                constants.RJ_ESCRITORIO_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "credentials_secret_name": "LGPD_SERVICE_ACCOUNT_B64",
                "dataset_id": "datalake_gestao",
                "dump_mode": "append",
                "last_execution_redis_key": "lgpd_iam_audit_log_last_execution",
                "redis_url_secret_name": "LGPD_REDIS_URL",
                "table_id": "iam_audit_log",
            },
        ),
    ]
)
