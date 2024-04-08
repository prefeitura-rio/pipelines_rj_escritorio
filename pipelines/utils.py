# -*- coding: utf-8 -*-
import base64
from os import environ
from typing import Any, Callable, Union

import prefect
from prefeitura_rio.pipelines_utils.infisical import get_infisical_client, inject_env
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode


def inject_bd_credentials(environment: str = "staging") -> None:
    """
    Loads Base dos Dados credentials from Infisical into environment variables.
    """
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

    service_account_name = f"BASEDOSDADOS_CREDENTIALS_{environment.upper()}"
    service_account = base64.b64decode(environ[service_account_name])
    with open("/tmp/credentials.json", "wb") as credentials_file:
        credentials_file.write(service_account)
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
