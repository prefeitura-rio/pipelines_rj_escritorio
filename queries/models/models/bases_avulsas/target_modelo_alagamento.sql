CREATE OR REPLACE TABLE `rj-escritorio-dev.bases_avulsas.target_modelo_alagamento` AS 
WITH 
  dim_timestamp AS (
    SELECT timestamp_truncado FROM
    UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP('2016-01-01 00:00:00'), TIMESTAMP('2022-08-01 00:00:00'), INTERVAL 1 HOUR)) timestamp_truncado 
    ),
  contagem_eventos_hora AS (
    SELECT
      d.timestamp_truncado, 
      SUM( 
        CASE WHEN id_pop IN ('33', '32', '31', '6', '5') THEN 1 ELSE 0 END) as target_eventos_alag_hora,
      COUNT(DISTINCT id_evento) AS eventos_total_hora
    FROM `rj-cor.administracao_servicos_publicos.eventos` ev
    RIGHT JOIN dim_timestamp d
      on d.timestamp_truncado= TIMESTAMP_TRUNC(TIMESTAMP(ev.data_inicio), HOUR) AND ev.data_particao >= '2016-01-01'
    GROUP BY 
      d.timestamp_truncado
    ORDER BY 
      d.timestamp_truncado
  )
-- Quantos eventos irão acontecer dali 2h?
SELECT timestamp_truncado,
  target_eventos_alag_hora,  -- qnt de alagamentos que aconteceram naquela hora
  CASE WHEN SUM(target_eventos_alag_hora) OVER (ORDER BY timestamp_truncado ASC ROWS BETWEEN 1 FOLLOWING AND 6 FOLLOWING) >= 1 THEN 1 ELSE 0 END AS target_evento_prox_6_h, -- pelo menos um evento dentro das próximas 6h
  LEAD(target_eventos_alag_hora, 2) OVER (ORDER BY timestamp_truncado ASC) AS target_eventos_alag_2h, -- pelo menos um evento acontecendo dali 2h
  LEAD(target_eventos_alag_hora, 6) OVER (ORDER BY timestamp_truncado ASC) AS target_eventos_alag_6h, -- pelo menos um evento acontecendo dali 6h
  LEAD(target_eventos_alag_hora, 12) OVER (ORDER BY timestamp_truncado ASC) AS target_eventos_alag_12h, -- pelo menos um evento acontecendo dali 12h
  LEAD(target_eventos_alag_hora, 24) OVER (ORDER BY timestamp_truncado ASC) AS target_eventos_alag_24h, -- pelo menos um evento acontecendo dali 24h
  LEAD(eventos_total_hora, 2) OVER (ORDER BY timestamp_truncado ASC) AS target_eventos_2h, -- qnt de eventos acontecendo dali 2h
  LEAD(eventos_total_hora, 6) OVER (ORDER BY timestamp_truncado ASC) AS target_eventos_6h, -- qnt de eventos acontecendo dali 6h
  LEAD(eventos_total_hora, 12) OVER (ORDER BY timestamp_truncado ASC) AS target_eventos_12h, -- qnt de eventos acontecendo dali 12h
  LEAD(eventos_total_hora, 24) OVER (ORDER BY timestamp_truncado ASC) AS target_eventos_24h -- qnt de eventos acontecendo dali 24h

FROM contagem_eventos_hora
ORDER BY 
    timestamp_truncado
;

CREATE OR REPLACE TABLE `rj-escritorio-dev.bases_avulsas.main_table_modelo_alagamento` AS (

SELECT t.*, f.* 
FROM `rj-escritorio-dev.bases_avulsas.target_modelo_alagamento` t
LEFT JOIN `rj-escritorio-dev.bases_avulsas.features_modelo_alagamento` f ON SAFE_CAST(f.ts AS TIMESTAMP) = t.timestamp_truncado