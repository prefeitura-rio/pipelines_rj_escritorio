# -*- coding: utf-8 -*-
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log


@task
def hello_someone(name: str) -> None:
    log(f"Hello, {name}!", "info")
