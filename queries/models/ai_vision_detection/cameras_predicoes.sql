SELECT
  CAST(datetime AS DATETIME) AS datetime,
  id_camera,
  url_camera,
  model,
  object,
  label,
  confidence,
  prompt,
  CAST(max_output_token AS INT64) AS max_output_token,
  CAST(temperature AS FLOAT64) AS temperature,
  CAST(top_k AS INT64) AS top_k,
  CAST(top_p AS INT64) AS top_p,
  CAST(latitude AS FLOAT64) AS latitude,
  CAST(longitude AS FLOAT64) AS longitude,
  ST_GEOGPOINT(
    CAST(longitude AS FLOAT64),
    CAST(CAST(longitude AS FLOAT64) AS FLOAT64)
  ) AS geometry,
  image_base64,
  CAST(data_particao AS DATE) data_particao,
FROM `rj-escritorio-dev.ai_vision_detection_staging.cameras_predicoes`
