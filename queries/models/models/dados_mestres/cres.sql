SELECT 
    SAFE_CAST(TRIM(nome) AS STRING) AS nome,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(codigo ,'0') , r'\.0$', '') AS STRING) id,
    SAFE_CAST(geometry_wkt AS STRING) geometry_wkt,
    SAFE.ST_GEOGFROMTEXT(geometry) geometry
FROM rj-escritorio-dev.dados_mestres_staging.cres AS t