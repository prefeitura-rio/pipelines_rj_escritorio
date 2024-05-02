# -*- coding: utf-8 -*-
from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.constants import constants
from pipelines.sandbox.async_requests.tasks import get_random_cat_pictures

with Flow(
    name="Sandbox: Test async requests",
    state_handlers=[handler_inject_bd_credentials],
) as sandbox__async_requests_flow:
    # Parameters
    batch_size = Parameter("batch_size", default=50)
    n_times = Parameter("n_times", default=500)

    get_random_cat_pictures(n_times=n_times, batch_size=batch_size)

sandbox__async_requests_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sandbox__async_requests_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=["escritoriodedados"],
)
