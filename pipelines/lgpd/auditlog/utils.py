# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
from google.cloud import logging_v2
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.lgpd.tables_bindings.utils import get_gcp_credentials


def extract_iam_audit_logs(
    *, project_id: str, secret_name: str, start: datetime, end: datetime
) -> list:
    client = get_logging_client(secret_name=secret_name)
    parent = f"projects/{project_id}"
    filter_ = (
        'log_id("cloudaudit.googleapis.com/activity") AND protoPayload.methodName:"SetIamPolicy"'
    )
    filter_ += f" AND timestamp>=\"{start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}\""
    filter_ += f" AND timestamp<=\"{end.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}\""
    log(f"Filter: {filter_}")

    response = client.list_entries(resource_names=[parent], filter_=filter_)

    entries = []
    for entry in response:
        entries.append(entry)

    log(f"Found {len(entries)} entries")
    return entries


def get_logging_client(secret_name: str) -> logging_v2.Client:
    """
    Get a logging client.

    Returns:
        logging_v2.Client: A logging client.
    """
    credentials = get_gcp_credentials(secret_name=secret_name)
    return logging_v2.Client(credentials=credentials)


def parse_iam_audit_logs(entries: list) -> pd.DataFrame:
    """
    Parse IAM audit logs.

    Args:
        entries (list): A list of log entries.

    Returns:
        List[dict]: A list of parsed log entries.
    """
    parsed_entries = []
    for entry in entries:
        payload = entry.payload
        principal = payload.get("authenticationInfo", {}).get("principalEmail")
        resource = payload.get("resourceName")
        timestamp = entry.timestamp
        response = payload.get("response")
        for binding in response.get("bindings", []):
            role = binding.get("role")
            members = binding.get("members", [])
            for member in members:
                parsed_entries.append(
                    {
                        "data_particao": timestamp.strftime("%Y-%m-%d"),
                        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "principal": principal,
                        "resource": resource,
                        "role": role,
                        "member": member,
                    }
                )
    return pd.DataFrame(parsed_entries)
