with
    _source_wetalkie as (

        select
            safe.parse_date('%d-%m-%Y', json_value(data, '$.date')) as date,
            safe.parse_timestamp(
                '%d-%m-%YT%H:%M:%S',
                concat(json_value(data, '$.date'), 'T', json_value(data, '$.time'))
            ) as timestamp,
            nullif(json_value(data, '$.session_id'), "") as session_id,
            nullif(json_value(data, '$.num_wpp'), "") as num_wpp,
            nullif(json_value(data, '$.query'), "") as query,
            nullif(json_value(data, '$.transcript'), "") as transcript,
            nullif(json_value(data, '$.ai_response.ai_overview'), "") as ai_overview,
            nullif(json_value(data, '$.portal_origem'), "") as portal_origem,
            nullif(json_value(data, '$.tipo_dispositivo'), "") as tipo_dispositivo,
            json_extract_array(data, '$.ai_response.titles') as titles,
            json_extract_array(data, '$.api_results') as api_results,
            nullif(json_value(data, '$.audio_gcs_uri'), "") as audio_gcs_uri,
            cast(nullif(json_value(data, '$.llm_reorder'), "") as bool) as llm_reorder,
            nullif(json_value(data, '$.ai_response.prompt_raw'), "") as prompt_raw,
            nullif(
                json_value(data, '$.ai_response.system_prompt'), ""
            ) as system_prompt,
            safe_cast(
                json_value(data, '$.ai_response.temperature') as numeric
            ) as temperature,
            safe_cast(json_value(data, '$.ai_response.top_k') as int64) as top_k,
            safe_cast(json_value(data, '$.ai_response.top_p') as numeric) as top_p

        from `rj-chatbot.wetalkie.buscas_staging`
    )

select *
from _source_wetalkie
