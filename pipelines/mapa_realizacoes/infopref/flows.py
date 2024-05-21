# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.constants import constants
from pipelines.mapa_realizacoes.infopref.tasks import (
    cleanup_unused,
    compute_aggregate_data,
    get_bairros_with_geometry,
    get_firestore_client,
    get_gmaps_key,
    get_infopref_cidade,
    get_infopref_headers,
    get_infopref_orgao,
    get_infopref_programa,
    get_infopref_realizacao_raw,
    get_infopref_status,
    get_infopref_tema,
    get_infopref_url,
    load_firestore_credential_to_file,
    log_task,
    transform_infopref_realizacao_to_firebase,
    upload_aggregated_data_to_firestore,
    upload_infopref_data_to_firestore,
)

with Flow(
    name="Mapa de Realizações - Atualiza banco de dados do Firestore",
    state_handlers=[handler_inject_bd_credentials],
    skip_if_running=True,
    parallelism=50,
) as rj_escritorio__mapa_realizacoes__infopref__flow:
    # Parameters
    clear = Parameter("clear", default=True)
    firestore_credentials_secret_name = Parameter(
        "firestore_credentials_secret_name", default="FIRESTORE_CREDENTIALS"
    )
    gmaps_secret_name = Parameter("gmaps_secret_name", default="GMAPS_KEY")
    infopref_header_token_secret_name = Parameter(
        "infopref_header_token_secret_name", default="INFOPREF_TOKEN"
    )
    infopref_url_orgao_secret_name = Parameter(
        "infopref_url_orgao_secret_name", default="INFOPREF_URL_ORGAO"
    )
    infopref_url_programa_secret_name = Parameter(
        "infopref_url_programa_secret_name", default="INFOPREF_URL_PROGRAMA"
    )
    infopref_url_realizacao_secret_name = Parameter(
        "infopref_url_realizacao_secret_name", default="INFOPREF_URL_REALIZACAO"
    )
    infopref_url_tema_secret_name = Parameter(
        "infopref_url_tema_secret_name", default="INFOPREF_URL_TEMA"
    )

    # Tasks
    gmaps_key = get_gmaps_key(secret_name=gmaps_secret_name)
    infopref_headers = get_infopref_headers(token_secret=infopref_header_token_secret_name)
    infopref_url_orgao = get_infopref_url(url_secret=infopref_url_orgao_secret_name)
    infopref_url_programa = get_infopref_url(url_secret=infopref_url_programa_secret_name)
    infopref_url_realizacao = get_infopref_url(url_secret=infopref_url_realizacao_secret_name)
    infopref_url_tema = get_infopref_url(url_secret=infopref_url_tema_secret_name)

    cidades = get_infopref_cidade()
    orgaos = get_infopref_orgao(url_orgao=infopref_url_orgao, headers=infopref_headers)
    programas = get_infopref_programa(url_programa=infopref_url_programa, headers=infopref_headers)
    raw_realizacoes = get_infopref_realizacao_raw(
        url_realizacao=infopref_url_realizacao, headers=infopref_headers
    )
    statuses = get_infopref_status()
    temas = get_infopref_tema(url_tema=infopref_url_tema, headers=infopref_headers)

    firestore_credentials_task = load_firestore_credential_to_file(
        secret_name=firestore_credentials_secret_name
    )
    db = get_firestore_client()
    db.set_upstream(firestore_credentials_task)
    bairros = get_bairros_with_geometry(db=db)

    realizacoes = transform_infopref_realizacao_to_firebase.map(
        entry=raw_realizacoes,
        gmaps_key=unmapped(gmaps_key),
        db=unmapped(db),
        bairros=unmapped(bairros),
    )

    clean_cidades, clean_orgaos, clean_programas, clean_statuses, clean_temas = cleanup_unused(
        cidades=cidades,
        orgaos=orgaos,
        programas=programas,
        realizacoes=realizacoes,
        statuses=statuses,
        temas=temas,
    )

    aggregated_data = compute_aggregate_data(realizacoes=realizacoes)

    upload_aggregated_data_task = upload_aggregated_data_to_firestore(
        data=aggregated_data, db=db, collection="aggregated_data", clear=clear
    )
    upload_cidades_task = upload_infopref_data_to_firestore(
        data=clean_cidades, db=db, collection="cidade", clear=clear
    )
    upload_cidades_task.set_upstream(firestore_credentials_task)

    upload_orgaos_task = upload_infopref_data_to_firestore(
        data=clean_orgaos, db=db, collection="orgao", clear=clear
    )
    upload_orgaos_task.set_upstream(firestore_credentials_task)

    upload_programas_task = upload_infopref_data_to_firestore(
        data=clean_programas, db=db, collection="programa", clear=clear
    )
    upload_programas_task.set_upstream(firestore_credentials_task)

    upload_realizacoes_task = upload_infopref_data_to_firestore(
        data=realizacoes, db=db, collection="realizacao", clear=clear
    )
    upload_realizacoes_task.set_upstream(firestore_credentials_task)
    upload_realizacoes_task.set_upstream(clean_statuses)

    upload_statuses_task = upload_infopref_data_to_firestore(
        data=clean_statuses, db=db, collection="status", clear=clear
    )
    upload_statuses_task.set_upstream(firestore_credentials_task)

    upload_temas_task = upload_infopref_data_to_firestore(
        data=clean_temas, db=db, collection="tema", clear=clear
    )
    upload_temas_task.set_upstream(firestore_credentials_task)

    log_task_ref = log_task(msg="This is the end.")
    log_task_ref.set_upstream(upload_aggregated_data_task)
    log_task_ref.set_upstream(upload_cidades_task)
    log_task_ref.set_upstream(upload_orgaos_task)
    log_task_ref.set_upstream(upload_programas_task)
    log_task_ref.set_upstream(upload_realizacoes_task)
    log_task_ref.set_upstream(upload_statuses_task)
    log_task_ref.set_upstream(upload_temas_task)

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
