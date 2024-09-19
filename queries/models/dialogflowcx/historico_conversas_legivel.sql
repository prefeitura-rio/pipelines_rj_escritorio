WITH historico_padrao AS (
  select
    conversation_name as conversa_completa_id,
    turn_position as conversa_completa_turn_position,
    conversation_name,
    turn_position,
    JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.text')) as mensagem_cidadao,
    JSON_VALUE(JSON_EXTRACT(derived_data, '$.agentUtterances')) as resposta_bot,
    FORMAT_DATETIME("%d/%m/%Y às %H:%M", DATETIME(request_time, "America/Buenos_Aires")) as horario,
    JSON_EXTRACT(response, '$.queryResult.parameters') as parametros,
    request_time,
    response
  from `rj-chatbot-dev.dialogflowcx.historico_conversas`
  ORDER BY conversation_name, request_time ASC
),

historico_macrofluxo AS (
  SELECT
    conversation_name as conversa_completa_id,
    turn_position as conversa_completa_turn_position,
    new_conversation_id as conversation_name,
    new_turn as turn_position,
    mensagem_cidadao,
    resposta_bot,
    horario,
    parametros,
    request_time,
    response
  FROM {{ ref('historico_conversas_macrofluxos') }}
  ORDER BY conversation_name, request_time ASC
)

(SELECT * FROM historico_padrao
UNION ALL
SELECT * FROM historico_macrofluxo)
ORDER BY conversation_name, request_time ASC