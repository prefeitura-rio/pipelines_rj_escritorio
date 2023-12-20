# -*- coding: utf-8 -*-
"""
Schedules for the data catalog pipeline.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

update_flooding_data_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=3),
            start_date=datetime(2023, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
            labels=[
                constants.RJ_ESCRITORIO_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "api_key_secret_path": "/flooding-detection",
                "cameras_geodf_url": "https://docs.google.com/spreadsheets/d/122uOaPr8YdW5PTzrxSPF-FD0tgco596HqgB7WK7cHFw/edit#gid=914166579",  # noqa
                "google_api_model": "gemini-pro-vision",
                "mocked_cameras_number": 0,
                "object_parameters_url": "https://docs.google.com/spreadsheets/d/122uOaPr8YdW5PTzrxSPF-FD0tgco596HqgB7WK7cHFw/edit#gid=1580662721",  # noqa
                "rain_api_update_url": "https://api.dados.rio/v2/clima_pluviometro/ultima_atualizacao_precipitacao_15min/",  # noqa
                "rain_api_url": "https://api.dados.rio/v2/clima_pluviometro/precipitacao_15min/",
                "redis_key_flooding_detection_data": "flooding_detection_data",
                "redis_key_flooding_detection_last_update": "flooding_detection_last_update",
                "redis_key_predictions_buffer": "flooding_detection_predictions_buffer",
            },
        ),
    ]
)
