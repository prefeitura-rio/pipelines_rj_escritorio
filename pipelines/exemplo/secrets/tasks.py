# -*- coding: utf-8 -*-
from prefect import task
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.logging import log


@task
def read_secret(secret_name):
    return get_secret(secret_name)


@task
def print_secret(secret):
    log(secret)
