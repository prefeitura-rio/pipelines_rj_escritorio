{{
    config(
        materialized="table",
        cluster_by="cnpj",
        partition_by={
            "field": "cnpj_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 99999999999999, "interval": 34722222},
        },
    )
}}

WITH
dicionario_identificador_matriz_filial AS (
    SELECT
        chave AS chave_identificador_matriz_filial,
        valor AS descricao_identificador_matriz_filial
    FROM `basedosdados.br_me_cnpj.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'identificador_matriz_filial'
        AND id_tabela = 'estabelecimentos'
),
dicionario_situacao_cadastral AS (
    SELECT
        chave AS chave_situacao_cadastral,
        valor AS descricao_situacao_cadastral
    FROM `basedosdados.br_me_cnpj.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'situacao_cadastral'
        AND id_tabela = 'estabelecimentos'
),
dicionario_id_pais AS (
    SELECT
        chave AS chave_id_pais,
        valor AS descricao_id_pais
    FROM `basedosdados.br_me_cnpj.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'id_pais'
        AND id_tabela = 'estabelecimentos'
)
SELECT
    dados.data as data,
    dados.cnpj as cnpj,
    dados.cnpj_basico as cnpj_basico,
    dados.cnpj_ordem as cnpj_ordem,
    dados.cnpj_dv as cnpj_dv,
    descricao_identificador_matriz_filial AS identificador_matriz_filial,
    dados.nome_fantasia as nome_fantasia,
    descricao_situacao_cadastral AS situacao_cadastral,
    dados.data_situacao_cadastral as data_situacao_cadastral,
    dados.motivo_situacao_cadastral as motivo_situacao_cadastral,
    dados.nome_cidade_exterior as nome_cidade_exterior,
    descricao_id_pais AS id_pais,
    dados.data_inicio_atividade as data_inicio_atividade,
    dados.cnae_fiscal_principal as cnae_fiscal_principal,
    dados.cnae_fiscal_secundaria as cnae_fiscal_secundaria,
    dados.sigla_uf AS sigla_uf,
    diretorio_sigla_uf.nome AS sigla_uf_nome,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    dados.id_municipio_rf as id_municipio_rf,
    dados.tipo_logradouro as tipo_logradouro,
    dados.logradouro as logradouro,
    dados.numero as numero,
    dados.complemento as complemento,
    dados.bairro as bairro,
    dados.cep as cep,
    dados.ddd_1 as ddd_1,
    dados.telefone_1 as telefone_1,
    dados.ddd_2 as ddd_2,
    dados.telefone_2 as telefone_2,
    dados.ddd_fax as ddd_fax,
    dados.fax as fax,
    dados.email as email,
    dados.situacao_especial as situacao_especial,
    dados.data_situacao_especial as data_situacao_especial,
    SAFE_CAST(dados.cnpj AS INT64) AS cnpj_particao
FROM `basedosdados.br_me_cnpj.estabelecimentos` AS dados
LEFT JOIN `dicionario_identificador_matriz_filial`
    ON dados.identificador_matriz_filial = chave_identificador_matriz_filial
LEFT JOIN `dicionario_situacao_cadastral`
    ON dados.situacao_cadastral = chave_situacao_cadastral
LEFT JOIN `dicionario_id_pais`
    ON dados.id_pais = chave_id_pais
LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
    ON dados.sigla_uf = diretorio_sigla_uf.sigla
LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
WHERE dados.sigla_uf = 'RJ' AND dados.id_municipio = '3304557'
