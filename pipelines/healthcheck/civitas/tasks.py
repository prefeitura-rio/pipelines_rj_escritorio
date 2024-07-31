# -*- coding: utf-8 -*-
import requests
from prefect import task
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.logging import log


@task
def get_status_data_relay(environment: str = "prod") -> dict:
    """
    Get the status of the data relay service

    Args:
        environment (str): The environment to check the service status. Default is "prod".

    Returns:
        dict: The status of the data relay service.
    """
    if environment not in ["staging", "prod"]:
        raise ValueError("Invalid environment. Please provide a valid environment.")

    base_url_map = {
        "staging": "https://staging.data-relay.dados.rio",
        "prod": "https://data-relay.dados.rio",
    }

    base_url = base_url_map[environment]
    url = f"{base_url}/health"

    try:
        response = requests.get(url)
        response.raise_for_status()
        status = response.json()
        log(f"Data relay service status: {status}", "info")
    except requests.exceptions.RequestException as exc:
        log(f"Failed to get data relay service status: {exc}", "error")
        status = {"status": "ERROR"}

    return status


@task
def get_status_civitas_api(environment: str = "prod") -> bool:
    """
    Get the status of the Civitas API

    Args:
        environment (str): The environment to check the service status. Default is "prod".

    Returns:
        bool: The status of the Civitas API.
    """
    if environment not in ["staging", "prod"]:
        raise ValueError("Invalid environment. Please provide a valid environment.")

    base_url_map = {
        "staging": "https://staging.api.civitas.rio",
        "prod": "https://api.civitas.rio",
    }

    base_url = base_url_map[environment]
    url = f"{base_url}/health"

    try:
        response = requests.get(url)
        response.raise_for_status()
        status = response.json()
        log(f"Civitas API status: {status}", "info")
        if status["status"] == "OK":
            return True
        return False
    except requests.exceptions.RequestException as exc:
        log(f"Failed to get Civitas API status: {exc}", "error")
        return False


@task
def generate_message(
    status_data_relay: dict,
    status_civitas_api: bool,
    min_messages_warning: int = 50,
    min_messages_critical: int = 100,
) -> str:
    """
    Generate a message based on the status of the services.

    Args:
        status_data_relay (dict): The status of the data relay service.
        status_civitas_api (bool): The status of the Civitas API.
        min_messages_warning (int): The minimum number of messages to trigger a warning. Default is
            50.
        min_messages_critical (int): The minimum number of messages to trigger a critical alert.
            Default is 100.

    Returns:
        str: The message to be sent through Discord webhook.
    """
    # Start with a header message and a placeholder for the final emoji
    message = "<emoji> **Healthcheck report:**\n\n"

    # Initialize final emoji
    emoji = "✅"

    # Check the status of the data relay service
    if status_data_relay["status"] != "OK":
        message += "- ❌ Data relay service is **down**.\n"
        emoji = "❌"
    else:
        message += "- Data relay service is **up**.\n"
        for queue in status_data_relay["queues"]:
            messages_to_process = queue["messages_ready"]
            queue_emoji = ""
            if messages_to_process >= min_messages_critical:
                queue_emoji = "❌ "
                emoji = "❌"
            elif messages_to_process >= min_messages_warning:
                queue_emoji = "⚠️ "
                emoji = "⚠️"
            message += (
                f"  - {queue_emoji}{queue['name']}: {messages_to_process} messages to process\n"
            )

    message += "\n"

    # Check the status of the Civitas API
    if status_civitas_api:
        message += "- Civitas API is **up**.\n"
    else:
        message += "- ❌ Civitas API is **down**.\n"
        emoji = "❌"

    # Replace the placeholder with the final emoji
    message = message.replace("<emoji>", emoji)

    return message


@task
def get_discord_webhook_url(secret_name: str) -> str:
    return get_secret(secret_name)[secret_name]


@task
def send_message_to_discord(message: str, webhook_url: str) -> None:
    """
    Send a message to a Discord webhook.

    Args:
        message (str): The message to be sent.
        webhook_url (str): The Discord webhook URL.
    """
    try:
        response = requests.post(webhook_url, json={"content": message})
        response.raise_for_status()
        log("Message sent to Discord successfully.", "info")
    except requests.exceptions.RequestException as exc:
        log(f"Failed to send message to Discord: {exc}", "error")
        raise
