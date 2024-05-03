# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pandas as pd
from prefect import task
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.logging import log
from redis import Redis

from pipelines.lgpd.auditlog.utils import extract_iam_audit_logs, parse_iam_audit_logs


@task(
    checkpoint=False,
    max_retries=5,
    retry_delay=timedelta(seconds=30),
)
def get_auditlog_dataframe(
    project_id: str,
    credentials_secret_name: str,
    start: datetime = None,
    end: datetime = None,
) -> pd.DataFrame:
    start = start or datetime(2021, 1, 1)
    end = end or datetime.now()
    log(f"Getting audit logs from {start} to {end}")
    entries = extract_iam_audit_logs(
        project_id=project_id, secret_name=credentials_secret_name, start=start, end=end
    )
    return parse_iam_audit_logs(entries=entries)


@task(checkpoint=False)
def get_last_execution_datetime(redis_url: str, key: str) -> datetime:
    redis = Redis.from_url(redis_url)
    last_execution = redis.get(key)
    if last_execution:
        ret_value = datetime.fromisoformat(last_execution.decode())
        log(f"Last execution: {ret_value}")
        return ret_value
    return None


@task(checkpoint=False)
def get_now() -> datetime:
    return datetime.now()


@task
def get_redis_url(secret_name: str) -> str:
    return get_secret(secret_name)[secret_name]


@task
def set_last_execution_datetime(redis_url: str, key: str, value: datetime):
    redis = Redis.from_url(redis_url)
    redis.set(key, value.isoformat())
