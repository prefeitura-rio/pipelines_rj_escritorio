# -*- coding: utf-8 -*-
import base64
import json
from typing import Any, List, Tuple

from google.cloud import bigquery
from google.oauth2 import service_account
from gspread.worksheet import Worksheet
from prefeitura_rio.pipelines_utils.infisical import get_secret


def get_gcp_credentials(secret_name: str, scopes: List[str] = None) -> service_account.Credentials:
    """
    Get GCP credentials from a secret.

    Args:
        secret_name: The secret name.
        scopes: The scopes to use.

    Returns:
        service_account.Credentials: The GCP credentials.
    """
    secret = get_secret(secret_name)[secret_name]
    info = json.loads(base64.b64decode(secret))
    credentials = service_account.Credentials.from_service_account_info(info)
    if scopes:
        credentials = credentials.with_scopes(scopes)
    return credentials


def list_tables(project_id: str) -> List[str]:
    """List all tables in a given project. The output is a list of strings in the format
    `//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}`.
    """
    client = bigquery.Client(project=project_id)
    datasets = client.list_datasets()
    tables = []

    for dataset in datasets:
        dataset_ref = client.dataset(dataset.dataset_id)
        dataset_tables = client.list_tables(dataset_ref)
        for table in dataset_tables:
            tables.append(
                f"//bigquery.googleapis.com/projects/{project_id}/datasets/{table.dataset_id}/tables/{table.table_id}"  # noqa
            )

    return tables


def parse_table_name(table: str) -> Tuple[str, str, str]:
    """
    Parse a table name from the format
    `//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}`
    to a tuple with the project_id, dataset_id and table_id.

    Args:
        table (str): The table name.

    Returns:
        Tuple[str, str, str]: A tuple with the project_id, dataset_id and table_id.
    """
    table = table.lstrip("//bigquery.googleapis.com/projects/")
    parts = table.split("/")
    project_id = parts[0]
    dataset_id = parts[2]
    table_id = parts[4]
    return project_id, dataset_id, table_id


def write_data_to_gsheets(worksheet: Worksheet, data: List[List[Any]], start_cell: str = "A1"):
    """
    Write data to a Google Sheets worksheet.

    Args:
        worksheet: Google Sheets worksheet.
        data: List of lists of data.
        start_cell: Cell to start writing data.
    """
    try:
        start_letter = start_cell[0]
        start_row = int(start_cell[1:])
    except ValueError as exc:
        raise ValueError("Invalid start_cell. Please use a cell like A1.") from exc
    cols_len = len(data[0])
    rows_len = len(data)
    end_letter = chr(ord(start_letter) + cols_len - 1)
    if end_letter not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        raise ValueError("Too many columns. Please refactor this code.")
    end_row = start_row + rows_len - 1
    range_name = f"{start_letter}{start_row}:{end_letter}{end_row}"
    worksheet.update(range_name, data)
