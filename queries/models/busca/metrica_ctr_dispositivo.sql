-- CTR por Dispositivo: Comparar click-through rate entre mobile/desktop.
-- Exemplo: Usuários mobile podem ter menor CTR se a UI for menos intuitiva
with
    _source_buscas as (
        select
            safe.parse_date('%d-%m-%Y', json_value(data, '$.date')) as search_date,
            nullif(json_value(data, '$.tipo_dispositivo'), "") as tipo_dispositivo
        -- Necessário: date, tipo_dispositivo
        from `rj-chatbot.busca.buscas`
        where
            safe.parse_date('%d-%m-%Y', json_value(data, '$.date')) is not null
            and nullif(json_value(data, '$.tipo_dispositivo'), "") is not null  -- Garante que o dispositivo é conhecido
    ),
    _source_cliques as (
        select
            safe.parse_date('%d-%m-%Y', json_value(data, '$.date')) as click_date,
            nullif(json_value(data, '$.tipo_dispositivo'), "") as tipo_dispositivo
        -- Necessário: date, tipo_dispositivo
        from `rj-chatbot.busca.cliques`
        where
            safe.parse_date('%d-%m-%Y', json_value(data, '$.date')) is not null
            and nullif(json_value(data, '$.tipo_dispositivo'), "") is not null  -- Garante que o dispositivo é conhecido
    ),
    -- Passo 1: Contar buscas por dia e dispositivo
    searches_per_day_device as (
        select search_date, tipo_dispositivo, count(*) as total_searches
        from _source_buscas
        group by search_date, tipo_dispositivo
    ),
    -- Passo 2: Contar cliques por dia e dispositivo
    clicks_per_day_device as (
        select click_date, tipo_dispositivo, count(*) as total_clicks
        from _source_cliques
        group by click_date, tipo_dispositivo
    )
-- Passo 3: Juntar as contagens e calcular o CTR por dia e dispositivo
select
    s.search_date as date,
    s.tipo_dispositivo,
    s.total_searches,
    coalesce(c.total_clicks, 0) as total_clicks,  -- Trata casos onde há buscas mas não cliques
    safe_divide(
        cast(coalesce(c.total_clicks, 0) as float64),  -- Numerador: Cliques
        cast(s.total_searches as float64)  -- Denominador: Buscas
    ) as ctr_por_dispositivo  -- Click Through Rate
from searches_per_day_device s
left join
    clicks_per_day_device c
    on s.search_date = c.click_date
    and s.tipo_dispositivo = c.tipo_dispositivo  -- Junta por data E dispositivo
order by date, tipo_dispositivo  -- Ordena para melhor visualização
;
