SELECT 
    SAFE_CAST(TRIM(legenda) AS STRING) AS legenda_setor,
    SAFE_CAST(TRIM(sigla) AS STRING) AS sigla_setor,
    SAFE_CAST(TRIM(nome) AS STRING) AS nome_setor,
    SAFE_CAST(TRIM(legislacao) AS STRING) AS legislacao,
    SAFE_CAST(TRIM(abrev) AS STRING) AS abreviacao,
    SAFE_CAST(TRIM(observacao) AS STRING) AS observacao,
    SAFE_CAST(TRIM(legislacao_extenso) AS STRING) AS legislacao_extenso,
    SAFE_CAST(REGEXP_REPLACE(st_areashape, r',', '.') AS FLOAT64) AS area,
    SAFE_CAST(REGEXP_REPLACE(st_perimetershape, r',', '.') AS FLOAT64) AS perimetro,
    SAFE_CAST(geometry_wkt AS STRING) geometry_wkt,
    SAFE.ST_GEOGFROMTEXT(geometry) geometry
FROM rj-escritorio-dev.dados_mestres_staging.zoneamento_setores AS t