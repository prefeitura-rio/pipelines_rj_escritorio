# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.constants import constants
from pipelines.exemplo.objetivo2.tasks import hello_someone

with Flow(
    name="SANDBOX",
    state_handlers=[handler_inject_bd_credentials],
    skip_if_running=True,
    parallelism=5,
) as rj_escritorio__exemplo__objetivo2__flow:
    # Parameters
    name = Parameter("name", default="world")

    # Flow
    hello_someone(name=name)


rj_escritorio__exemplo__objetivo2__flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio__exemplo__objetivo2__flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
