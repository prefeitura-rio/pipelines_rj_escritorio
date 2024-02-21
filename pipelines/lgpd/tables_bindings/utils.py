# -*- coding: utf-8 -*-
import base64
import json
from time import sleep
from typing import Any, List, Tuple

import pandas as pd
from google.api_core.exceptions import FailedPrecondition, NotFound
from google.cloud import asset, bigquery
from google.cloud.asset_v1.types.asset_service import (
    BatchGetEffectiveIamPoliciesResponse,
)
from google.oauth2 import service_account
from gspread.worksheet import Worksheet
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.logging import log


def batch_get_effective_iam_policies(
    client: asset.AssetServiceClient,
    scope: str,
    names: List[str],
    retry_delay: float = 7.5,
    backoff_factor: float = 2,
    max_retries: int = 5,
) -> pd.DataFrame:
    """
    Batch get effective IAM policies.

    Args:
        scope: The scope.
        names: The names.
        retry_delay: The retry delay.
        backoff_factor: The backoff factor for exponential backoff.
        max_retries: The maximum number of retries.

    Returns:
        pd.DataFrame: A DataFrame with the IAM policies for the given tables. The dataframe contains
            the following columns:
            - project_id: The project ID.
            - dataset_id: The dataset ID.
            - table_id: The table ID.
            - attached_resource: The resource to which the policy is attached.
            - role: The role for the binding.
            - member: The member for the binding.
    """
    success = False
    retries = 0
    while not success and retries < max_retries:
        try:
            request = asset.BatchGetEffectiveIamPoliciesRequest(scope=scope, names=names)
            response = client.batch_get_effective_iam_policies(request=request)
            return build_dataframe_from_batch_get_effective_iam_policies_response(response)
        except FailedPrecondition as exc:
            # This is a quota issue. We should wait and retry.
            log(
                f"Reached API quota. Retrying in {retry_delay * (backoff_factor**retries)} seconds."
            )
            sleep(retry_delay * (backoff_factor**retries))
            retries += 1
            if retries >= max_retries:
                raise FailedPrecondition(
                    f"Failed to get effective IAM policies after {max_retries} attempts."
                ) from exc
        except NotFound:
            # A resource was not found. We must handle the situation this way:
            # - If len(names) > 1, we must split the list in half and call the function recursively.
            # - If len(names) == 1, we must log the error and return an empty response.
            if len(names) > 1:
                log(
                    f"Some resources were not found. Splitting the list (size={len(names)}) and retrying."  # noqa
                )
                half = len(names) // 2
                left = names[:half]
                right = names[half:]
                left_df = batch_get_effective_iam_policies(client=client, scope=scope, names=left)
                right_df = batch_get_effective_iam_policies(client=client, scope=scope, names=right)
                return merge_dataframes_fn([left_df, right_df])
            else:
                log(f"Resource {names[0]} not found. Skipping.", level="warning")
                return pd.DataFrame()


def build_dataframe_from_batch_get_effective_iam_policies_response(
    response: BatchGetEffectiveIamPoliciesResponse,
) -> pd.DataFrame:
    """
    Build a DataFrame from a BatchGetEffectiveIamPoliciesResponse.

    Args:
        response: The response.

    Returns:
        pd.DataFrame: A DataFrame with the IAM policies for the given tables. The dataframe contains
            the following columns:
            - project_id: The project ID.
            - dataset_id: The dataset ID.
            - table_id: The table ID.
            - attached_resource: The resource to which the policy is attached.
            - role: The role for the binding.
            - member: The member for the binding.
    """
    policies = []
    for policy_result in response.policy_results:
        project_id, dataset_id, table_id = parse_table_name(policy_result.full_resource_name)
        for policy_info in policy_result.policies:
            attached_resource = policy_info.attached_resource
            policy = policy_info.policy
            for binding in policy.bindings:
                role = binding.role
                for member in binding.members:
                    policies.append(
                        {
                            "project_id": project_id,
                            "dataset_id": dataset_id,
                            "table_id": table_id,
                            "attached_resource": attached_resource,
                            "role": role,
                            "member": member,
                        }
                    )
    return pd.DataFrame(policies)


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


def list_tables(project_id: str, credentials: service_account.Credentials) -> List[str]:
    """List all tables in a given project. The output is a list of strings in the format
    `//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}`.
    """
    client = bigquery.Client(project=project_id, credentials=credentials)
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


def merge_dataframes_fn(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Merge a list of DataFrames into a single DataFrame.

    Args:
        dfs (List[pd.DataFrame]): The DataFrames to merge.

    Returns:
        pd.DataFrame: The merged DataFrame.
    """
    log(f"Merging {len(dfs)} DataFrames.")
    return pd.concat(dfs, ignore_index=True)


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
