-- Profundidade da Sessão: Número médio de buscas por sessão (reflete a necessidade de
-- refinamento da query).
-- Como medir: Contar queries únicas por session_id
with
    _source_buscas as (
        select
            nullif(json_value(data, '$.session_id'), "") as session_id,
            safe.parse_date('%d-%m-%Y', json_value(data, '$.date')) as date,
            nullif(json_value(data, '$.query'), "") as query
        from `rj-chatbot.busca.buscas`
        where
            safe.parse_date('%d-%m-%Y', json_value(data, '$.date')) is not null
            and nullif(json_value(data, '$.session_id'), "") is not null  -- Necessário para agrupar
            and nullif(json_value(data, '$.query'), "") is not null  -- Ignora buscas vazias
    ),
    searches_per_session_daily as (
        -- Conta queries ÚNICAS por sessão e por dia
        select date, session_id, count(distinct query) as num_unique_searches
        from _source_buscas
        group by date, session_id
    )
select date, avg(num_unique_searches) as profundidade_media_sessao
from searches_per_session_daily
group by date
order by date desc
