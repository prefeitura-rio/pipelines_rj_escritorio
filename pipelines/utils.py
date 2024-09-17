# -*- coding: utf-8 -*-
import base64
from os import environ
from pathlib import Path
from typing import Any, Callable, Union

import prefect
from prefeitura_rio.pipelines_utils.infisical import get_infisical_client, inject_env
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode


def inject_bd_credentials(environment: str = "dev", force_injection=False) -> None:
    """
    Loads Base dos Dados credentials from Infisical into environment variables.

    Args:
        environment (str, optional): The infiscal environment for which to retrieve credentials.
            Defaults to 'dev'. Accepts 'dev' or 'prod'.

    Returns:
        None
    """
    # Verify if all environment variables are already set
    all_variables_set = True
    for variable in [
        "BASEDOSDADOS_CONFIG",
        "BASEDOSDADOS_CREDENTIALS_PROD",
        "BASEDOSDADOS_CREDENTIALS_STAGING",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ]:
        if not environ.get(variable):
            all_variables_set = False
            break

    # If all variables are set, skip injection
    if all_variables_set and not force_injection:
        log("All environment variables are already set. Skipping injection.")
        return

    # Else inject the variables
    client = get_infisical_client()

    log(f"ENVIROMENT: {environment}")
    for secret_name in [
        "BASEDOSDADOS_CONFIG",
        "BASEDOSDADOS_CREDENTIALS_PROD",
        "BASEDOSDADOS_CREDENTIALS_STAGING",
    ]:
        inject_env(
            secret_name=secret_name,
            environment=environment,
            client=client,
        )

    # Create service account file for Google Cloud
    service_account_name = "BASEDOSDADOS_CREDENTIALS_PROD"
    credentials = base64.b64decode(environ[service_account_name])

    if not Path("/tmp").exists():
        Path("/tmp").mkdir()

    with open("/tmp/credentials.json", "wb") as credentials_file:
        credentials_file.write(credentials)
    environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/credentials.json"


def authenticated_task(
    fn: Callable = None, **task_init_kwargs: Any
) -> Union[
    prefect.tasks.core.function.FunctionTask,
    Callable[[Callable], prefect.tasks.core.function.FunctionTask],
]:
    """
    This was adapted from the original implementation in `pipelines_rj_sms` repository.

    A function that can be used to create a Prefect task.

    Mode 1: Standard Mode
    - If `fn` is not None, it creates a FunctionTask from `fn` and `task_init_kwargs`.

    Mode 2: Decorator Mode
    - If `fn` is None, it returns a decorator that can be used to create a Prefect task.
    - This case is used when we want to create a Prefect task from a function using @task()
    """

    def inject_credential_setting_in_function(function):
        """
        Receives a function and return a new version of it that injects the BD credentials
        in the beginning.
        """

        def new_function(**kwargs):
            env = get_flow_run_mode()
            log(f"[Injected] Set BD credentials for environment {env}", "debug")
            inject_bd_credentials(environment=env)

            log("[Injected] Now executing function normally...", "debug")
            return function(**kwargs)

        new_function.__name__ = function.__name__

        return new_function

    # Standard Mode: only create a FunctionTask from function
    if fn is not None:
        return prefect.tasks.core.function.FunctionTask(
            fn=inject_credential_setting_in_function(fn), **task_init_kwargs
        )
    # Decorator Mode: create a decoretor that can be used to create a Prefect task
    else:
        return lambda any_function: prefect.tasks.core.function.FunctionTask(
            fn=inject_credential_setting_in_function(any_function),
            **task_init_kwargs,
        )
