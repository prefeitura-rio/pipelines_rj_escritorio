# -*- coding: utf-8 -*-
from datetime import datetime
from pathlib import Path
from typing import List
from uuid import uuid4

import pandas as pd
from google.cloud import asset
from googleapiclient import discovery
from prefect import task
from prefeitura_rio.pipelines_utils.bd import create_table_and_upload_to_gcs
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.pandas import to_partitions

from pipelines.lgpd.tables_bindings.utils import (
    batch_get_effective_iam_policies,
    get_gcp_credentials,
    list_tables,
    merge_dataframes_fn,
)


@task
def list_projects(credentials_secret_name: str) -> List[str]:
    """
    Lists all GCP projects that we have access to.

    Args:
        mode: Credentials mode.
        exclude_dev: Exclude projects that ends with "-dev".

    Returns:
        List of project IDs.
    """
    credentials = get_gcp_credentials(secret_name=credentials_secret_name)
    service = discovery.build("cloudresourcemanager", "v1", credentials=credentials)
    request = service.projects().list()
    projects = []
    while request is not None:
        response = request.execute()
        for project in response.get("projects", []):
            project_id = project["projectId"]
            log(f"Found project {project_id}.")
            projects.append(project_id)
        request = service.projects().list_next(previous_request=request, previous_response=response)
    log(f"Found {len(projects)} projects.")
    return projects


@task
def get_project_tables_iam_policies(project_id: str, credentials_secret_name: str) -> pd.DataFrame:
    """
    Get IAM policies for a list of tables in a given project.

    Args:
        project_id (str): The project ID.

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
    credentials = get_gcp_credentials(secret_name=credentials_secret_name)
    tables = list_tables(project_id, credentials)
    log(f"Found {len(tables)} tables in project {project_id}.")
    client = asset.AssetServiceClient(credentials=credentials)
    scope = f"projects/{project_id}"
    # Split tables in batches of 20 (maximum allowed by the API)
    tables_batches = [tables[i : i + 20] for i in range(0, len(tables), 20)]  # noqa
    dfs = []
    for i, table_batch in enumerate(tables_batches):
        log(
            f"Getting IAM policies for batch {i + 1}/{len(tables_batches)} (project_id={project_id})."  # noqa
        )
        df_batch = batch_get_effective_iam_policies(client=client, scope=scope, names=table_batch)
        dfs.append(df_batch)
    if len(dfs) == 0:
        log(f"No IAM policies found for project {project_id}.")
        return pd.DataFrame()
    df = merge_dataframes_fn(dfs)
    log(f"Found {len(df)} IAM policies for project {project_id}.")
    return df


@task
def merge_dataframes(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Merge a list of DataFrames into a single DataFrame.

    Args:
        dfs (List[pd.DataFrame]): The DataFrames to merge.

    Returns:
        pd.DataFrame: The merged DataFrame.
    """
    log(f"Merging {len(dfs)} DataFrames.")
    return merge_dataframes_fn(dfs)


@task
def upload_dataframe_to_bigquery(
    dataframe: pd.DataFrame,
    dataset_id: str,
    table_id: str,
    dump_mode: str,
) -> None:
    """
    Upload a DataFrame to BigQuery.

    Args:
        dataframe: The DataFrame to upload.
    """
    save_files_dir = Path(f"/tmp/{uuid4()}")
    dataframe["data_particao"] = datetime.now().strftime("%Y-%m-%d")
    log(f"Generating partitioned files in {save_files_dir}.")
    to_partitions(
        data=dataframe,
        partition_columns=["data_particao"],
        savepath=save_files_dir,
    )
    log("Uploading partitioned files to BigQuery.")
    create_table_and_upload_to_gcs(
        data_path=save_files_dir,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=True,
    )
    log("Done.")
