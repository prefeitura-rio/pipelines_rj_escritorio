{{
    config(
        materialized='incremental',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}

SELECT
    SAFE_CAST(endereco_completo AS STRING) endereco_completo,
    SAFE_CAST(pais AS STRING) pais,
    SAFE_CAST(estado AS STRING) estado,
    SAFE_CAST(municipio AS STRING) municipio,
    SAFE_CAST(bairro AS STRING) bairro,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(id_logradouro ,'0'), r'\.0$', '') AS STRING) id_logradouro,
    SAFE_CAST(logradouro AS STRING) logradouro,
    SAFE_CAST(SAFE_CAST(numero_porta AS FLOAT64) AS INT64) numero_porta,
    SAFE_CAST(latitude AS FLOAT64) latitude,
    SAFE_CAST(longitude AS FLOAT64) longitude,
    ST_GEOGPOINT(SAFE_CAST(longitude AS FLOAT64), SAFE_CAST(latitude AS FLOAT64)) AS geometry,
    SAFE_CAST(data_particao AS DATE) data_particao,
FROM rj-escritorio-dev.dados_mestres_staging.enderecos_geolocalizados AS t
WHERE
    data_particao < CURRENT_DATE('America/Sao_Paulo')

{% if is_incremental() %}

{% set max_partition = run_query("SELECT gr FROM (SELECT IF(max(data_particao) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(data_particao)) as gr FROM " ~ this ~ ")").columns[0].values()[0] %}

AND
    data_particao > ("{{ max_partition }}")

{% endif %}