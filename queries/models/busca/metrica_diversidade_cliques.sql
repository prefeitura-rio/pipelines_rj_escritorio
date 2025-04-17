-- Diversidade de Cliques: Número médio de resultados únicos clicados por query (evita
-- "monocultura" de cliques no topo).
-- Como medir: Contar objeto clicado distintos por query
-- Valor Alto: Sugere que, em média, para uma determinada query, os usuários estão
-- clicando em uma variedade maior de resultados diferentes. Isso pode ser bom (vários
-- resultados relevantes) ou ruim (nenhum resultado é claramente o melhor, forçando
-- exploração).
-- Valor Baixo (próximo de 1): Sugere que, em média, para uma determinada query, os
-- usuários tendem a clicar quase sempre no mesmo resultado (baixa diversidade,
-- "monocultura"). Isso pode ser bom (o resultado de topo é excelente) ou ruim (outros
-- resultados relevantes podem não estar sendo vistos ou clicados)
with
    _source_cliques as (
        select
            safe.parse_date(
                '%d-%m-%Y', replace(json_value(data, '$.date'), '/', '-')
            ) as click_date,
            nullif(json_value(data, '$.query'), "") as query_term,  -- O termo buscado associado ao clique
            nullif(json_value(data, '$.objeto_clicado.id'), "") as clicked_object_id
        from `rj-chatbot.busca.cliques`
        -- Filtros para garantir que os dados necessários para a métrica estão presentes
        where
            safe.parse_date('%d-%m-%Y', replace(json_value(data, '$.date'), '/', '-'))
            is not null
            and nullif(json_value(data, '$.query'), "") is not null  -- Query não pode ser nula
            and nullif(json_value(data, '$.objeto_clicado.id'), "") is not null  -- ID do objeto não pode ser nulo
    ),
    -- Passo 1: Contar quantos objetos clicados DISTINTOS existem para cada query em
    -- cada dia
    distinct_objects_per_query_day as (
        select
            click_date,
            query_term,
            count(distinct clicked_object_id) as distinct_clicked_objects_count
        from _source_cliques
        group by click_date, query_term
    )
-- Passo 2: Calcular a média diária dessa contagem de objetos distintos por query
select
    click_date as date,
    -- Média da contagem de resultados únicos clicados, calculada sobre todas as
    -- queries daquele dia
    avg(
        cast(distinct_clicked_objects_count as float64)
    ) as avg_click_diversity_per_query
from distinct_objects_per_query_day
group by click_date
order by date
