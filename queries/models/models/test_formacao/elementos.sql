SELECT 
  SAFE_CAST(data as DATE) as data,
  SAFE_CAST(number as INT64) as numero,
  element as elemento,
FROM `rj-escritorio-dev.test_formacao_staging.test_table_2`
WHERE data < "{{var('ELEMENTOS_DATA_INICIAL')}}"