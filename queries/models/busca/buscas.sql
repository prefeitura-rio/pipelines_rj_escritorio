-- SELECT
-- distinct
-- key
-- FROM `rj-chatbot.busca.buscas`,
-- UNNEST(JSON_KEYS(data)) AS key
with
    _source_buscas as (
        select
            updated_at,
            nullif(json_value(data, '$.session_id'), "") as session_id,
            safe.parse_date(
                '%d-%m-%Y', replace(json_value(data, '$.date'), '/', '-')
            ) as date,
            safe.parse_timestamp(
                '%d-%m-%YT%H:%M:%S',
                concat(
                    replace(json_value(data, '$.date'), '/', '-'),
                    'T',
                    json_value(data, '$.time')
                )
            ) as timestamp,
            nullif(json_value(data, '$.portal_origem'), "") as portal_origem,
            nullif(json_value(data, '$.query'), "") as query,
            nullif(json_value(data, '$.tipo_dispositivo'), "") as tipo_dispositivo
        from `rj-chatbot.busca.buscas`
    )

select updated_at, session_id, date, timestamp, portal_origem, tipo_dispositivo, query
from _source_buscas
