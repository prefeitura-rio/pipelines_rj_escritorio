# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.constants import constants
from pipelines.mapa_realizacoes.infopref.tasks import (
    get_bairros_with_geometry,
    get_firestore_client,
    get_gmaps_key,
    get_infopref_data,
    get_infopref_headers,
    get_infopref_url,
    load_firestore_credential_to_file,
    transform_infopref_to_firebase,
    upload_infopref_data_to_firestore,
)

with Flow(
    name="Mapa de Realizações - Atualiza banco de dados do Firestore",
    state_handlers=[handler_inject_bd_credentials],
    skip_if_running=True,
    parallelism=50,
) as rj_escritorio__mapa_realizacoes__infopref__flow:
    # Parameters
    firestore_credentials_secret_name = Parameter(
        "firestore_credentials_secret_name", default="FIRESTORE_CREDENTIALS"
    )
    gmaps_secret_name = Parameter("gmaps_secret_name", default="GMAPS_KEY")
    infopref_header_token_secret_name = Parameter(
        "infopref_header_token_secret_name", default="INFOPREF_TOKEN"
    )
    infopref_url_secret_name = Parameter("infopref_url_secret_name", default="INFOPREF_URL")

    # Tasks
    gmaps_key = get_gmaps_key(secret_name=gmaps_secret_name)
    infopref_headers = get_infopref_headers(token_secret=infopref_header_token_secret_name)
    infopref_url = get_infopref_url(url_secret=infopref_url_secret_name)
    infopref_data = get_infopref_data(url=infopref_url, headers=infopref_headers)
    firestore_credentials_task = load_firestore_credential_to_file(
        secret_name=firestore_credentials_secret_name
    )
    db = get_firestore_client()
    db.set_upstream(firestore_credentials_task)
    bairros = get_bairros_with_geometry(db=db)
    transformed_infopref_data = transform_infopref_to_firebase.map(
        entry=infopref_data,
        gmaps_key=unmapped(gmaps_key),
        db=unmapped(db),
        bairros=unmapped(bairros),
    )
    upload_infopref_data_task = upload_infopref_data_to_firestore(
        data=transformed_infopref_data, db=db
    )
    upload_infopref_data_task.set_upstream(firestore_credentials_task)

rj_escritorio__mapa_realizacoes__infopref__flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio__mapa_realizacoes__infopref__flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
    cpu_request="1",
    cpu_limit="1",
    memory_request="2Gi",
    memory_limit="2Gi",
)
# TODO: add schedule
# rj_escritorio__mapa_realizacoes__infopref__flow.schedule = None
