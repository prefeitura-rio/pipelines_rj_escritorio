# -*- coding: utf-8 -*-
"""
Schedules for the data catalog pipeline.
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

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
                "cameras_geodf_url": "https://docs.google.com/spreadsheets/d/122uOaPr8YdW5PTzrxSPF-FD0tgco596HqgB7WK7cHFw/edit#gid=1580662721",  # noqa
                "mocked_cameras_number": 0,
                "api_key_secret_path": "/flooding-detection",
                "google_api_max_output_tokens": 300,
                "google_api_model": "gemini-pro-vision",
                "rain_api_update_url": "https://api.dados.rio/v2/clima_pluviometro/ultima_atualizacao_precipitacao_15min/",  # noqa
                "rain_api_url": "https://api.dados.rio/v2/clima_pluviometro/precipitacao_15min/",
                "redis_key_flooding_detection_data": "flooding_detection_data",
                "redis_key_flooding_detection_last_update": "flooding_detection_last_update",
                "redis_key_predictions_buffer": "flooding_detection_predictions_buffer",
            },
        ),
    ]
)
