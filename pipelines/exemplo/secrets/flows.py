# -*- coding: utf-8 -*-
from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.exemplo.secrets.tasks import print_secret, read_secret

with Flow(
    name="rj-escritorio: Nome do objetivo - Descrição detalhada do objetivo",
) as exemplo__secrets__print_flow:
    # Parameters
    secret_name = Parameter("secret_name", required=True)

    # Tasks
    secret_value = read_secret(secret_name=secret_name)
    print_secret(secret=secret_value)

# Storage and run configs
exemplo__secrets__print_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
exemplo__secrets__print_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
