{{
    config(
        materialized="table",
        cluster_by="cnpj",
        partition_by={
            "field": "cnpj_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 99999999999999, "interval": 34722222223},
        },
    )
}}

with
    tb_dados as (
        select *,
        from `basedosdados.br_me_cnpj.estabelecimentos`
        where sigla_uf = 'RJ' and id_municipio = '3304557'
        qualify row_number() over (partition by cnpj order by data desc) = 1 -- get only the most recent data
    ),

    dicionario_tipo as (
        select chave as chave_tipo, valor as descricao_tipo
        from `basedosdados.br_me_cnpj.dicionario`
        where true and nome_coluna = 'tipo' and id_tabela = 'socios'
    ),
    dicionario_qualificacao as (
        select chave as chave_qualificacao, valor as descricao_qualificacao
        from `basedosdados.br_me_cnpj.dicionario`
        where true and nome_coluna = 'qualificacao' and id_tabela = 'socios'
    ),
    dicionario_id_pais_socios as (
        select chave as chave_id_pais, valor as descricao_id_pais
        from `basedosdados.br_me_cnpj.dicionario`
        where true and nome_coluna = 'id_pais' and id_tabela = 'socios'
    ),
    dicionario_qualificacao_representante_legal as (
        select
            chave as chave_qualificacao_representante_legal,
            valor as descricao_qualificacao_representante_legal
        from `basedosdados.br_me_cnpj.dicionario`
        where
            true
            and nome_coluna = 'qualificacao_representante_legal'
            and id_tabela = 'socios'
    ),

    dicionario_faixa_etaria as (
        select chave as chave_faixa_etaria, valor as descricao_faixa_etaria
        from `basedosdados.br_me_cnpj.dicionario`
        where true and nome_coluna = 'faixa_etaria' and id_tabela = 'socios'
    ),

    socios as (
        select
            dados.data as data,
            dados.cnpj_basico as cnpj_basico,
            array_agg(
                struct(
                    descricao_tipo as tipo,
                    dados.nome as nome,
                    dados.documento as documento,
                    descricao_qualificacao as qualificacao,
                    dados.data_entrada_sociedade as data_entrada_sociedade,
                    descricao_id_pais as pais,
                    dados.cpf_representante_legal as cpf_representante_legal,
                    dados.nome_representante_legal as nome_representante_legal,
                    descricao_qualificacao_representante_legal
                    as qualificacao_representante_legal,
                    descricao_faixa_etaria as faixa_etaria
                )
            ) as socios
        from `basedosdados.br_me_cnpj.socios` as dados
        left join `dicionario_tipo` on dados.tipo = chave_tipo
        left join `dicionario_qualificacao` on dados.qualificacao = chave_qualificacao
        left join `dicionario_id_pais_socios` on dados.id_pais = chave_id_pais
        left join
            `dicionario_qualificacao_representante_legal`
            on dados.qualificacao_representante_legal
            = chave_qualificacao_representante_legal
        left join `dicionario_faixa_etaria` on dados.faixa_etaria = chave_faixa_etaria
        where dados.cnpj_basico in (select distinct cnpj_basico from tb_dados)
        group by dados.data, dados.cnpj_basico
    ),

    dicionario_identificador_matriz_filial as (
        select
            chave as chave_identificador_matriz_filial,
            valor as descricao_identificador_matriz_filial
        from `basedosdados.br_me_cnpj.dicionario`
        where
            true
            and nome_coluna = 'identificador_matriz_filial'
            and id_tabela = 'estabelecimentos'
    ),

    dicionario_situacao_cadastral as (
        select chave as chave_situacao_cadastral, valor as descricao_situacao_cadastral
        from `basedosdados.br_me_cnpj.dicionario`
        where
            true
            and nome_coluna = 'situacao_cadastral'
            and id_tabela = 'estabelecimentos'
    ),

    dicionario_id_pais as (
        select chave as chave_id_pais, valor as descricao_id_pais
        from `basedosdados.br_me_cnpj.dicionario`
        where true and nome_coluna = 'id_pais' and id_tabela = 'estabelecimentos'
    ),

    cnae_tb as (
        select
            data,
            cnpj,
            array_agg(
                struct(
                    cnae,
                    c.descricao_subclasse,
                    c.descricao_classe,
                    c.descricao_grupo,
                    c.descricao_divisao,
                    c.descricao_secao
                )
            ) as cnae
        from
            tb_dados e,
            unnest(
                split(
                    concat(
                        ifnull(e.cnae_fiscal_principal, 'null'),
                        ',',
                        ifnull(e.cnae_fiscal_secundaria, 'null')
                    ),
                    ','
                )
            ) as cnae
        left join `basedosdados.br_bd_diretorios_brasil.cnae_2` c on cnae = c.subclasse
        where cnae != 'null'
        group by data, cnpj
    )

select
    dados.data as data,
    dados.cnpj as cnpj,
    dados.cnpj_basico as cnpj_basico,
    dados.cnpj_ordem as cnpj_ordem,
    dados.cnpj_dv as cnpj_dv,
    descricao_identificador_matriz_filial as identificador_matriz_filial,
    dados.nome_fantasia as nome_fantasia,
    descricao_situacao_cadastral as situacao_cadastral,
    dados.data_situacao_cadastral as data_situacao_cadastral,
    dados.motivo_situacao_cadastral as motivo_situacao_cadastral,
    dados.nome_cidade_exterior as nome_cidade_exterior,
    descricao_id_pais as pais,
    dados.data_inicio_atividade as data_inicio_atividade,
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
    c.cnae,
    s.socios,
    safe_cast(dados.cnpj as int64) as cnpj_particao,
from tb_dados as dados
left join
    dicionario_identificador_matriz_filial
    on dados.identificador_matriz_filial = chave_identificador_matriz_filial
left join
    dicionario_situacao_cadastral
    on dados.situacao_cadastral = chave_situacao_cadastral
left join dicionario_id_pais on dados.id_pais = chave_id_pais
left join socios s on dados.cnpj_basico = s.cnpj_basico and dados.data = s.data
left join cnae_tb c on dados.cnpj = c.cnpj and dados.data = c.data

