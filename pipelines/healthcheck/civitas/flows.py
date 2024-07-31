# -*- coding: utf-8 -*-
from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.healthcheck.civitas.schedules import every_five_minutes
from pipelines.healthcheck.civitas.tasks import (
    generate_message,
    get_discord_webhook_url,
    get_status_civitas_api,
    get_status_data_relay,
    send_message_to_discord,
)

with Flow(
    name="Healthcheck: CIVITAS",
) as healthcheck__civitas__flow:
    # Parameters
    environment = Parameter("environment", default="prod")
    min_messages_critical = Parameter("min_messages_critical", default=100)
    min_messages_warning = Parameter("min_messages_warning", default=50)
    secret_name_discord_webhook_url = Parameter(
        "secret_name_discord_webhook_url", default="DISCORD_WEBHOOK_HEALTHCHECK_CIVITAS"
    )

    status_data_relay = get_status_data_relay(environment=environment)
    status_civitas_api = get_status_civitas_api(environment=environment)

    message = generate_message(
        status_data_relay=status_data_relay,
        status_civitas_api=status_civitas_api,
        min_messages_warning=min_messages_warning,
        min_messages_critical=min_messages_critical,
    )

    webhook_url = get_discord_webhook_url(secret_name=secret_name_discord_webhook_url)
    send_message_to_discord(message=message, webhook_url=webhook_url)

# Storage and run configs
healthcheck__civitas__flow.schedule = every_five_minutes
healthcheck__civitas__flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
healthcheck__civitas__flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value, labels=["escritoriodedados"]
)
