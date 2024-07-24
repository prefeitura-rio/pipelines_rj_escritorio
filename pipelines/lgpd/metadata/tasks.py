# -*- coding: utf-8 -*-
import pandas as pd
import requests
from prefect import task


@task(checkpoint=False)
def get_metadata_from_api(url: str) -> pd.DataFrame:
    # Get results from API
    data = requests.get(url).json()["results"]

    # Parse rows
    rows = []
    for project in data:
        project_id = project["name"]
        datasets = project["datasets"]
        for dataset in datasets:
            dataset_id = dataset["name"]
            tables = dataset["tables"]
            for table in tables:
                table_id = table["name"]
                table_title = table["title"]
                table_short_description = table["short_description"]
                table_long_description = table["long_description"]
                table_update_frequency = table["update_frequency"]
                table_temporal_coverage = table["temporal_coverage"]
                table_data_owner = table["data_owner"]
                table_publisher_name = table["publisher_name"]
                table_publisher_email = table["publisher_email"]
                columns = table["columns"]
                for column in columns:
                    column_name = column["name"]
                    column_description = column["description"]
                    column_type = column["type"]
                    rows.append(
                        [
                            project_id,
                            dataset_id,
                            table_id,
                            table_title,
                            table_short_description,
                            table_long_description,
                            table_update_frequency,
                            table_temporal_coverage,
                            table_data_owner,
                            table_publisher_name,
                            table_publisher_email,
                            column_name,
                            column_description,
                            column_type,
                        ]
                    )

    # Generate dataframe
    df = pd.DataFrame(
        rows,
        columns=[
            "project_id",
            "dataset_id",
            "table_id",
            "table_title",
            "table_short_description",
            "table_long_description",
            "table_update_frequency",
            "table_temporal_coverage",
            "table_data_owner",
            "table_publisher_name",
            "table_publisher_email",
            "column_name",
            "column_description",
            "column_type",
        ],
    )

    return df
