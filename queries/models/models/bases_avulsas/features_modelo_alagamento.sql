-- CREATE OR REPLACE TABLE `rj-escritorio-dev.bases_avulsas.features_modelo_alagamento` AS (
-- WITH total_alerta_rio as (
--   SELECT 
--     *, 
--     CAST(CONCAT(data_particao,' ',horario) AS DATETIME) ts 
--   FROM `rj-cor.meio_ambiente_clima.taxa_precipitacao_alertario` 
--   ORDER BY data_particao, id_estacao, horario DESC
-- )

-- , acumulados as (
--   SELECT 
--     t1.primary_key,
--     t1.id_estacao,
--     t1.acumulado_chuva_15_min,
--     t1.acumulado_chuva_1_h,
--     t1.acumulado_chuva_4_h,
--     ROUND(SUM(CASE WHEN DATETIME_DIFF(t1.ts, t2.ts, HOUR) < 12 THEN t2.acumulado_chuva_15_min ELSE NULL END),2) acumulado_chuva_12_h,
--     t1.acumulado_chuva_24_h,
--     t1.acumulado_chuva_96_h,
--     t1.horario,
--     t1.data_particao
--   FROM total_alerta_rio as t1
--   LEFT JOIN total as t2
--     ON t1.id_estacao = t2.id_estacao
--   GROUP BY   
--     t1.primary_key,
--     t1.id_estacao,
--     t1.acumulado_chuva_15_min,
--     t1.acumulado_chuva_1_h,
--     t1.acumulado_chuva_4_h,
--     t1.acumulado_chuva_24_h,
--     t1.acumulado_chuva_96_h,
--     t1.horario,
--     t1.data_particao
--   ORDER BY data_particao, id_estacao, horario DESC
-- )

WITH meteorologia as (
  SELECT 
    *, 
    CAST(CONCAT(data_particao,' ',horario) AS DATETIME) ts,
    EXTRACT(YEAR FROM data_particao) ano,
    EXTRACT(ISOWEEK FROM data_particao) semana,
    EXTRACT(DAYOFYEAR FROM data_particao) dia_do_ano
  FROM `rj-cor.meio_ambiente_clima.meteorologia_inmet`
  WHERE data_particao >= '2016-01-01'
)

, acumulados as (
  SELECT 
    t1.primary_key,
    t1.id_estacao,
    t1.data_particao,
    t1.horario,
    t1.acumulado_chuva_1_h,
    SUM(t1.acumulado_chuva_1_h)
      OVER (window_4h) AS acumulado_chuva_4_h,
    SUM(t1.acumulado_chuva_1_h)
      OVER (window_12h) AS acumulado_chuva_12_h,
    SUM(t1.acumulado_chuva_1_h)
      OVER (window_24h) AS acumulado_chuva_24_h
  FROM meteorologia as t1
  WINDOW 
    window_4h AS (
        PARTITION BY id_estacao
        ORDER BY ts
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW),
    window_12h AS (
        PARTITION BY id_estacao
        ORDER BY ts
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW),
    window_24h AS (
        PARTITION BY id_estacao
        ORDER BY ts
        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)
)

, eventos as (
  SELECT
    *,
    EXTRACT(YEAR FROM data_particao) ano,
    EXTRACT(ISOWEEK FROM data_particao) semana, 
  FROM `rj-cor.administracao_servicos_publicos.eventos`
  WHERE id_pop IN ('5', '6', '31', '32', '33') and data_particao >= '2016-01-01'
  ORDER BY id_evento
)

, soma_ocorrencias_semana_0 as (
  SELECT 
    ano,
    semana,
    COUNT(id_evento) soma_ocorrencias
  FROM eventos
  GROUP BY ano, semana
  ORDER BY ano, semana
)

, soma_ocorrencias_semana as (
  SELECT 
    ano,
    semana,
    soma_ocorrencias,
    LAG(soma_ocorrencias,1) OVER (ORDER BY ano, semana) as soma_ocorrencias_1_semana_atras,
    LAG(soma_ocorrencias,2) OVER (ORDER BY ano, semana) as soma_ocorrencias_2_semana_atras,
    LAG(soma_ocorrencias,3) OVER (ORDER BY ano, semana) as soma_ocorrencias_3_semana_atras,
    LAG(soma_ocorrencias,1) OVER (ORDER BY ano, semana)/(LAG(soma_ocorrencias,2) OVER (ORDER BY ano, semana) + LAG(soma_ocorrencias,3) OVER (ORDER BY ano, semana)) as taxa_evolucao_ocorrencias
  FROM soma_ocorrencias_semana_0
)

SELECT
  t1.primary_key,
  t1.ts,
  t1.id_estacao,
  ac.acumulado_chuva_1_h,
  ac.acumulado_chuva_4_h,
  ac.acumulado_chuva_12_h,
  ac.acumulado_chuva_24_h,
  t1.temperatura,
  LAG(t1.temperatura_maxima,3) OVER (PARTITION BY t1.id_estacao ORDER BY ts) as max_temperatura_4h,
  LAG(t1.temperatura_maxima,11) OVER (PARTITION BY t1.id_estacao ORDER BY ts) as max_temperatura_12h,
  LAG(t1.temperatura_maxima,23) OVER (PARTITION BY t1.id_estacao ORDER BY ts) as max_temperatura_24h,
  t1.velocidade_vento,
  LAG(t1.velocidade_vento,1) OVER (PARTITION BY t1.id_estacao ORDER BY ts) as velocidade_vento_1h,
  LAG(t1.velocidade_vento,4) OVER (PARTITION BY t1.id_estacao ORDER BY ts) as velocidade_vento_4h,
  LAG(t1.velocidade_vento,12) OVER (PARTITION BY t1.id_estacao ORDER BY ts) as velocidade_vento_12h,
  LAG(t1.velocidade_vento,24) OVER (PARTITION BY t1.id_estacao ORDER BY ts) as velocidade_vento_24h,
  t1.pressao,
  LAG(t1.pressao_minima,1) OVER (PARTITION BY t1.id_estacao ORDER BY ts) as pressao_minima_1h,
  LAG(t1.pressao_minima,4) OVER (PARTITION BY t1.id_estacao ORDER BY ts) as pressao_minima_4h,
  LAG(t1.pressao_minima,12) OVER (PARTITION BY t1.id_estacao ORDER BY ts) as pressao_minima_12h,
  LAG(t1.pressao_minima,24) OVER (PARTITION BY t1.id_estacao ORDER BY ts) as pressao_minima_24h,
  sos.soma_ocorrencias as soma_ocorrencias_mesma_semana_ano_anterior,
  sos2.soma_ocorrencias_1_semana_atras,
  sos2.taxa_evolucao_ocorrencias,
  CASE WHEN dia_do_ano >= 80 AND dia_do_ano < 172 THEN 1 ELSE 0 END as outono,
  CASE WHEN dia_do_ano >= 172 AND dia_do_ano < 266 THEN 1 ELSE 0 END as inverno,
  CASE WHEN dia_do_ano >= 266 AND dia_do_ano < 356 THEN 1 ELSE 0 END as primavera,
  CASE WHEN dia_do_ano < 80 OR dia_do_ano >= 356 THEN 1 ELSE 0 END as verao,
  CASE WHEN t1.horario >= '6:00:00' AND t1.horario < '12:00:00' THEN 1 ELSE 0 END as manha,
  CASE WHEN t1.horario >= '12:00:00' AND t1.horario < '18:00:00' THEN 1 ELSE 0 END as tarde,
  CASE WHEN t1.horario >= '18:00:00' THEN 1 ELSE 0 END as noite,
  CASE WHEN t1.horario < '6:00:00' THEN 1 ELSE 0 END as madrugada,
  LEAST(ABS(t1.semana - 2), (53-t1.semana) + 2) as intervalo_ate_pico
FROM meteorologia as t1
LEFT JOIN acumulados as ac
  ON t1.primary_key = ac.primary_key
LEFT JOIN soma_ocorrencias_semana as sos
  ON (t1.ano - 1) = sos.ano AND t1.semana = sos.semana 
-- O join acima é pra garantir que estamos pegando a soma de ocorrências da mesma semana do ano anterior 
-- e não LAG(52). Imaginei que essa feature seria muito afetada no caso de missing data.
LEFT JOIN soma_ocorrencias_semana as sos2
  ON t1.ano = sos2.ano AND t1.semana = sos2.semana
ORDER BY t1.data_particao DESC, t1.id_estacao, t1.horario DESC
--)
