-- Taxa de Retorno à Busca: % de usuários que voltam a pesquisar após clicar em um
-- resultado (indica insatisfação com o resultado clicado).
-- Como medir: Comparar sessões com múltiplas buscas após um clique.
with
    _source_buscas as (
        select
            nullif(json_value(data, '$.session_id'), "") as session_id,
            safe.parse_date(
                '%d-%m-%Y', replace(json_value(data, '$.date'), '/', '-')
            ) as search_date,  -- Data da busca
            safe.parse_timestamp(
                '%d-%m-%YT%H:%M:%S',
                concat(
                    replace(json_value(data, '$.date'), '/', '-'),
                    'T',
                    json_value(data, '$.time')
                )
            ) as search_timestamp  -- Timestamp exato da busca
        -- Apenas session_id, date e timestamp são necessários das buscas
        from `rj-chatbot.busca.buscas`
        where
            json_value(data, '$.session_id') is not null  -- Ignora eventos sem session_id
            and replace(json_value(data, '$.date'), '/', '-') is not null
            and json_value(data, '$.time') is not null
    -- and json_value(data, '$.session_id') in
    -- ('34db7a33-b050-492c-9979-12d629e87f7b',
    -- '7ea120ce-90cf-4857-8c41-c0f38bf032b8')
    ),
    _source_cliques as (
        select
            nullif(json_value(data, '$.session_id'), "") as session_id,
            safe.parse_date(
                '%d-%m-%Y', replace(json_value(data, '$.date'), '/', '-')
            ) as click_date,  -- Data do clique
            safe.parse_timestamp(
                '%d-%m-%YT%H:%M:%S',
                concat(
                    replace(json_value(data, '$.date'), '/', '-'),
                    'T',
                    json_value(data, '$.time')
                )
            ) as click_timestamp  -- Timestamp exato do clique
        -- Apenas session_id, date e timestamp são necessários dos cliques
        from `rj-chatbot.busca.cliques`
        where
            json_value(data, '$.session_id') is not null  -- Ignora eventos sem session_id
            and replace(json_value(data, '$.date'), '/', '-') is not null
            and json_value(data, '$.time') is not null
    -- and json_value(data, '$.session_id') in
    -- ('34db7a33-b050-492c-9979-12d629e87f7b',
    -- '7ea120ce-90cf-4857-8c41-c0f38bf032b8')
    ),
    -- Passo 1: Identificar todas as sessões que tiveram pelo menos um clique em um
    -- determinado dia.
    -- Isso formará o DENOMINADOR da nossa taxa. Agrupamos pela data do clique.
    sessions_with_click_on_day as (
        select distinct
            click_date as date,  -- Agrupa pela data em que o clique ocorreu
            session_id
        from _source_cliques
        where click_date is not null and session_id is not null
    ),
    -- Contagem diária de sessões com cliques (DENOMINADOR)
    daily_sessions_with_click_count as (
        select date, count(session_id) as total_sessions_with_click
        from sessions_with_click_on_day
        group by date
    ),
    -- Passo 2: Identificar as sessões onde ocorreu uma busca *após* um clique.
    -- Comparamos os timestamps dentro da mesma sessão.
    -- Uma sessão pode ter múltiplos cliques e buscas. Queremos apenas saber SE existe
    -- *pelo menos uma* busca após *qualquer* clique.
    -- Agrupamos pela data da *segunda busca* (o retorno).
    sessions_returned_to_search as (
        select distinct
            b.search_date as date,  -- Data da busca que ocorreu *após* o clique
            b.session_id
        from _source_buscas b
        inner join _source_cliques c on b.session_id = c.session_id  -- Mesma sessão
        where
            b.search_timestamp > c.click_timestamp  -- A busca DEVE ocorrer DEPOIS do clique
            and b.search_date is not null  -- Garante que a data da busca de retorno é válida
            and b.session_id is not null
    ),
    -- Contagem diária de sessões que retornaram à busca (NUMERADOR)
    daily_returned_to_search_count as (
        select date, count(session_id) as returned_to_search_sessions
        from sessions_returned_to_search
        group by date
    )
-- Passo 3: Calcular a taxa diária
select
    denom.date,
    denom.total_sessions_with_click,
    coalesce(num.returned_to_search_sessions, 0) as returned_to_search_sessions,
    safe_divide(
        cast(coalesce(num.returned_to_search_sessions, 0) as float64),  -- Numerador
        cast(denom.total_sessions_with_click as float64)  -- Denominador
    ) as return_to_search_rate  -- Taxa de Retorno à Busca
from daily_sessions_with_click_count denom  -- Usa a contagem de sessões com clique como base
left join daily_returned_to_search_count num on denom.date = num.date  -- Junta com a contagem de sessões que retornaram
order by denom.date  -- Ordena por data
;
