with
    _source_wetalkie as (

        select
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
            nullif(json_value(data, '$.session_id'), "") as session_id,
            nullif(json_value(data, '$.num_wpp'), "") as num_wpp,
            nullif(json_value(data, '$.query_original'), "") as query_original,
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
                json_value(data, '$.ai_response.model_ai_overview'), ""
            ) as model_ai_overview,
            nullif(
                json_value(data, '$.ai_response.model_simplify_query'), ""
            ) as model_simplify_query,
            cast(
                nullif(
                    json_value(data, '$.ai_response.token_usage.input_tokens'), ""
                ) as int64
            ) as input_tokens,
            cast(
                nullif(
                    json_value(data, '$.ai_response.token_usage.output_tokens'), ""
                ) as int64
            ) as output_tokens,
            nullif(
                json_value(data, '$.ai_response.system_prompt'), ""
            ) as system_prompt,
            safe_cast(
                json_value(data, '$.ai_response.temperature') as numeric
            ) as temperature,
            safe_cast(json_value(data, '$.ai_response.top_k') as int64) as top_k,
            safe_cast(json_value(data, '$.ai_response.top_p') as numeric) as top_p

        from `rj-chatbot.wetalkie.buscas_staging`
    ),

    _source_wetalkie_avaliacoes as (
        select
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
            nullif(json_value(data, '$.session_id'), "") as session_id,
            nullif(json_value(data, '$.num_wpp'), "") as num_wpp,
            cast(nullif(json_value(data, '$.good_rate'), "") as bool) as good_rate
        from `rj-chatbot.wetalkie.avaliacoes_staging`
    )

select
    a.date,
    a.timestamp,
    a.session_id,
    a.num_wpp,
    a.query_original,
    a.query,
    a.transcript,
    case when a.transcript is null then 'texto' else 'audio' end as tipo_mensagem,
    a.ai_overview,
    a.portal_origem,
    a.tipo_dispositivo,
    a.titles,
    a.api_results,
    a.audio_gcs_uri,
    a.llm_reorder,
    a.model_ai_overview,
    a.model_simplify_query,
    a.input_tokens,
    a.output_tokens,
    a.prompt_raw,
    a.system_prompt,
    a.temperature,
    a.top_k,
    a.top_p,
    b.good_rate
from _source_wetalkie a
left join
    _source_wetalkie_avaliacoes b
    on a.session_id = b.session_id
    and a.num_wpp = b.num_wpp
