-- Taxa de Abandono por Portal: % de buscas sem cliques
with
    _source_buscas as (
        select
            nullif(json_value(data, '$.session_id'), "") as session_id,
            safe.parse_date(
                '%d-%m-%Y', replace(json_value(data, '$.date'), '/', '-')
            ) as search_date,
            safe.parse_timestamp(
                '%d-%m-%YT%H:%M:%S',
                concat(
                    replace(json_value(data, '$.date'), '/', '-'),
                    'T',
                    json_value(data, '$.time')
                )
            ) as search_timestamp,
            nullif(json_value(data, '$.portal_origem'), "") as portal_origem
        -- Campos necessários: session_id, date, timestamp, portal_origem
        from `rj-chatbot.busca.buscas`
        where
            json_value(data, '$.session_id') is not null
            and replace(json_value(data, '$.date'), '/', '-') is not null
            and json_value(data, '$.time') is not null
            and json_value(data, '$.portal_origem') is not null  -- Importante para o group by
    ),
    _source_cliques as (
        select
            nullif(json_value(data, '$.session_id'), "") as session_id,
            safe.parse_timestamp(
                '%d-%m-%YT%H:%M:%S',
                concat(
                    replace(json_value(data, '$.date'), '/', '-'),
                    'T',
                    json_value(data, '$.time')
                )
            ) as click_timestamp
        -- Campos necessários: session_id, timestamp (para comparar com busca)
        from `rj-chatbot.busca.cliques`
        where
            json_value(data, '$.session_id') is not null
            and replace(json_value(data, '$.date'), '/', '-') is not null
            and json_value(data, '$.time') is not null
    ),
    -- Passo 1: Para cada busca, encontrar o timestamp da próxima busca na mesma sessão
    searches_with_next_search_time as (
        select
            session_id,
            search_date,
            portal_origem,
            search_timestamp,
            -- Pega o timestamp da próxima linha (busca) dentro da mesma sessão,
            -- ordenado por tempo
            lead(search_timestamp) over (
                partition by session_id order by search_timestamp
            ) as next_search_timestamp
        from _source_buscas
    ),
    -- Passo 2: Verificar se existe um clique entre a busca atual e a próxima busca
    -- (ou após a última busca)
    search_click_interval_check as (
        select
            s.search_date,
            s.portal_origem,
            s.session_id,
            s.search_timestamp,
            s.next_search_timestamp,
            -- Verifica se existe (EXISTS) algum clique (c) na mesma sessão que:
            -- 1. Ocorreu DEPOIS da busca atual (c.click_timestamp > s.search_timestamp)
            -- 2. E ( Ocorreu ANTES da próxima busca OU a busca atual é a última na
            -- sessão )
            exists (
                select 1
                from _source_cliques c
                where
                    c.session_id = s.session_id
                    and c.click_timestamp > s.search_timestamp  -- Clique posterior à busca atual
                    and (
                        s.next_search_timestamp is null
                        or c.click_timestamp < s.next_search_timestamp
                    )  -- Clique anterior à próxima busca (ou não há próxima busca)
            ) as had_click_before_next_search
        from searches_with_next_search_time s
    )
-- Passo 3: Agrupar por dia e portal para calcular as contagens e a taxa de abandono.
select
    search_date as date,
    portal_origem,
    count(distinct session_id) as usuarios_distintos,
    count(*) as total_searches,  -- Denominador: Total de buscas no dia/portal
    -- Numerador: Conta as buscas onde NÃO houve clique antes da próxima busca/fim da
    -- sessão
    sum(
        case when not had_click_before_next_search then 1 else 0 end
    ) as abandoned_searches,
    safe_divide(
        cast(
            sum(case when not had_click_before_next_search then 1 else 0 end) as float64
        ),  -- Numerador
        cast(count(*) as float64)  -- Denominador
    ) as abandonment_rate
from search_click_interval_check
where search_date is not null
group by date, portal_origem
order by date, portal_origem
