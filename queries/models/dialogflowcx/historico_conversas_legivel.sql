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
  WHERE conversation_name NOT IN (
          "projects/rj-chatbot-dev/locations/global/agents/29358e97-22d5-48e0-b6e0-fe32e70b67cd/environments/f288d64a-52f3-42f7-be7d-cac0b0f4957a/sessions/protocol-62510003786190",
          "projects/rj-chatbot-dev/locations/global/agents/29358e97-22d5-48e0-b6e0-fe32e70b67cd/environments/f288d64a-52f3-42f7-be7d-cac0b0f4957a/sessions/protocol-37270003820798",
          "projects/rj-chatbot-dev/locations/global/agents/29358e97-22d5-48e0-b6e0-fe32e70b67cd/environments/fcd7f325-6095-4ce1-9757-6cc028b8b554/sessions/protocol-73430003652037",
          "projects/rj-chatbot-dev/locations/global/agents/29358e97-22d5-48e0-b6e0-fe32e70b67cd/environments/f288d64a-52f3-42f7-be7d-cac0b0f4957a/sessions/protocol-38990003782307")
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

-- Combine rows where conversation_name does not exist in the other table
(
  SELECT hp.* 
  FROM historico_padrao hp
  LEFT JOIN historico_macrofluxo hm 
    ON hp.conversation_name = hm.conversation_name
  WHERE hm.conversation_name IS NULL

  UNION ALL

  SELECT hm.*
  FROM historico_macrofluxo hm
  LEFT JOIN historico_padrao hp
    ON hm.conversation_name = hp.conversation_name
  WHERE hp.conversation_name IS NULL
)

ORDER BY conversation_name, request_time ASC