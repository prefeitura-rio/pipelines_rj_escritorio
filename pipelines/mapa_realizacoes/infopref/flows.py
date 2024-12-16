# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.mapa_realizacoes.infopref.schedules import every_day_at_3am
from pipelines.mapa_realizacoes.infopref.tasks import (
    cleanup_unused,
    compute_aggregate_data,
    filter_out_nones,
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
    merge_lists,
    merge_lists_no_checkpoint,
    split_by_gestao,
    transform_csv_to_pin_only_realizacoes,
    transform_infopref_realizacao_to_firebase,
    upload_aggregated_data_to_firestore,
    upload_infopref_data_to_firestore,
)

with Flow(
    name="Mapa de Realizações - Atualiza banco de dados do Firestore",
    state_handlers=[handler_initialize_sentry, handler_inject_bd_credentials],
    skip_if_running=True,
    parallelism=50,
) as rj_escritorio__mapa_realizacoes__infopref__flow:
    # Parameters
    clear = Parameter("clear", default=True)
    firestore_credentials_secret_name = Parameter(
        "firestore_credentials_secret_name", default="FIRESTORE_CREDENTIALS"
    )
    force_pass = Parameter("force_pass", default=False)
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

    realizacoes_alarme_sonoro = transform_csv_to_pin_only_realizacoes(
        csv_url="https://storage.googleapis.com/datario-public/static/alarme_sonoro_v2.csv",
        id_tema="resiliência_climática",
        id_programa="sirenes",
        bairros=bairros,
        force_pass=force_pass,
        gestao="3",
    )

    realizacoes_alertario = transform_csv_to_pin_only_realizacoes(
        csv_url="https://storage.googleapis.com/datario-public/static/alertario.csv",
        id_tema="resiliência_climática",
        id_programa="estações_alerta_rio",
        bairros=bairros,
        force_pass=force_pass,
        gestao="3",
    )

    realizacoes_alarme_alertario = merge_lists_no_checkpoint(
        list_a=realizacoes_alarme_sonoro, list_b=realizacoes_alertario
    )

    realizacoes_cameras = transform_csv_to_pin_only_realizacoes(
        csv_url="https://storage.googleapis.com/datario-public/static/cameras.csv",
        id_tema="resiliência_climática",
        id_programa="câmeras",
        bairros=bairros,
        force_pass=force_pass,
        gestao="3",
    )

    realizacoes_pin_only = merge_lists_no_checkpoint(
        list_a=realizacoes_alarme_alertario, list_b=realizacoes_cameras
    )

    realizacoes = transform_infopref_realizacao_to_firebase.map(
        entry=raw_realizacoes,
        gmaps_key=unmapped(gmaps_key),
        db=unmapped(db),
        bairros=unmapped(bairros),
        temas=unmapped(temas),
        force_pass=unmapped(force_pass),
    )
    realizacoes.set_upstream(cidades)
    realizacoes.set_upstream(orgaos)
    realizacoes.set_upstream(programas)
    realizacoes.set_upstream(statuses)
    realizacoes.set_upstream(temas)
    realizacoes.set_upstream(realizacoes_pin_only)
    realizacoes_filtered = filter_out_nones(data=realizacoes)

    realizacoes_nova_gestao, realizacoes_gestoes_antigas = split_by_gestao(
        realizacoes=realizacoes_filtered
    )

    clean_cidades, clean_orgaos, clean_programas, clean_statuses, clean_temas = cleanup_unused(
        cidades=cidades,
        orgaos=orgaos,
        programas=programas,
        realizacoes=realizacoes_filtered,
        statuses=statuses,
        temas=temas,
    )

    all_programas = merge_lists(
        list_a=clean_programas,
        list_b=[
            {
                "id": "sirenes",
                "data": {
                    "descricao": None,
                    "id_tema": "resiliência_climática",
                    "image_url": None,
                    "nome": "Sirenes",
                },
            },
            {
                "id": "estações_alerta_rio",
                "data": {
                    "descricao": None,
                    "id_tema": "resiliência_climática",
                    "image_url": None,
                    "nome": "Estações Alerta Rio",
                },
            },
            {
                "id": "câmeras",
                "data": {
                    "descricao": None,
                    "id_tema": "resiliência_climática",
                    "image_url": None,
                    "nome": "Câmeras",
                },
            },
        ],
    )

    all_realizacoes = merge_lists_no_checkpoint(
        list_a=realizacoes_filtered, list_b=realizacoes_pin_only
    )

    aggregated_data = compute_aggregate_data(realizacoes=realizacoes_nova_gestao)
    aggregated_data_plano_verao = compute_aggregate_data(
        realizacoes=realizacoes_nova_gestao, version="PLANO_VERAO"
    )

    upload_aggregated_data_task = upload_aggregated_data_to_firestore(
        data=aggregated_data, db=db, collection="aggregated_data", clear=clear
    )
    upload_aggregated_data_planoverao_task = upload_aggregated_data_to_firestore(
        data=aggregated_data_plano_verao,
        db=db,
        collection="aggregated_data__planoverao",
        clear=clear,
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
        data=all_programas, db=db, collection="programa", clear=clear
    )
    upload_programas_task.set_upstream(firestore_credentials_task)

    upload_realizacoes_task = upload_infopref_data_to_firestore(
        data=all_realizacoes, db=db, collection="realizacao", clear=clear
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
    log_task_ref.set_upstream(upload_aggregated_data_planoverao_task)
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
rj_escritorio__mapa_realizacoes__infopref__flow.schedule = every_day_at_3am
