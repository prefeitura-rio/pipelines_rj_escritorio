-- Mean Reciprocal Rank (MRR): Média do inverso da posição do primeiro resultado
-- clicado.
-- Exemplo: Se o primeiro clique for na posição 2, MRR = 1/2.
-- Indica qualidade do ranking. Valores proximos a 1 = melhor ranking.
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
            ) as search_timestamp
        -- Necessário: session_id, date, timestamp
        from `rj-chatbot.busca.buscas`
        where
            nullif(json_value(data, '$.session_id'), "") is not null
            and safe.parse_date(
                '%d-%m-%Y', replace(json_value(data, '$.date'), '/', '-')
            )
            is not null
            and safe.parse_timestamp(
                '%d-%m-%YT%H:%M:%S',
                concat(
                    replace(json_value(data, '$.date'), '/', '-'),
                    'T',
                    json_value(data, '$.time')
                )
            )
            is not null
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
            ) as click_timestamp,
            cast(nullif(json_value(data, '$.posicao'), "") as int64) + 1 as posicao
        -- Necessário: session_id, timestamp, posicao
        from `rj-chatbot.busca.cliques`
        where
            nullif(json_value(data, '$.session_id'), "") is not null
            and safe.parse_timestamp(
                '%d-%m-%YT%H:%M:%S',
                concat(
                    replace(json_value(data, '$.date'), '/', '-'),
                    'T',
                    json_value(data, '$.time')
                )
            )
            is not null
            and cast(nullif(json_value(data, '$.posicao'), "") as int64) is not null
    ),
    -- Passo 1: Para cada busca, encontrar o timestamp da próxima busca na mesma sessão
    searches_with_next_time as (
        select
            session_id,
            search_date,
            search_timestamp,
            lead(search_timestamp) over (
                partition by session_id order by search_timestamp
            ) as next_search_timestamp
        from _source_buscas
    ),
    -- Passo 2: Para cada busca, encontrar TODOS os cliques que ocorreram antes da
    -- próxima busca
    clicks_in_search_window as (
        select
            s.search_date,
            s.session_id,
            s.search_timestamp,  -- Identificador único da busca
            c.click_timestamp,
            c.posicao
        from searches_with_next_time s
        inner join _source_cliques c on s.session_id = c.session_id
        where
            c.click_timestamp > s.search_timestamp  -- Clique ocorreu DEPOIS da busca
            and (
                s.next_search_timestamp is null
                or c.click_timestamp < s.next_search_timestamp
            )  -- Clique ocorreu ANTES da próxima busca (ou não há próxima busca)
    ),
    -- Passo 3: Rankear os cliques dentro da janela de cada busca para encontrar o
    -- primeiro
    ranked_clicks_per_search as (
        select
            search_date,
            session_id,
            search_timestamp,
            posicao,
            -- Rankeia os cliques por tempo para cada busca específica
            row_number() over (
                partition by session_id, search_timestamp order by click_timestamp asc
            ) as click_rank_after_search
        from clicks_in_search_window
    ),
    -- Passo 4: Filtrar apenas o primeiro clique (rank=1) e calcular o Reciprocal Rank
    -- para cada busca
    search_reciprocal_rank as (
        select
            search_date,
            session_id,
            search_timestamp,
            posicao,
            safe_divide(1.0, cast(posicao as float64)) as reciprocal_rank
        from ranked_clicks_per_search
        where click_rank_after_search = 1  -- Pega apenas o primeiro clique após a busca
    )
-- Passo 5: Calcular a média diária dos Reciprocal Ranks (MRR)
select
    search_date as date,
    -- Média dos RRs de todas as buscas que tiveram um clique relevante naquele dia
    avg(reciprocal_rank) as mean_reciprocal_rank
from search_reciprocal_rank
group by search_date
order by search_date
;
