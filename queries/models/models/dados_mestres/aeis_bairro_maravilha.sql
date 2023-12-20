
SELECT 
    SAFE_CAST(name AS STRING) nome_aies_bairro_maravilha,
    SAFE_CAST(folderpath AS STRING) folderpath, # verificar metadado
    SAFE_CAST(symbolid AS STRING) id_symbol, # verificar metadado
    SAFE_CAST(clamped AS STRING) clamped, # verificar metadado
    SAFE_CAST(bairro AS STRING) nome_bairro, 
    SAFE_CAST(sequencia AS STRING) sequencia,
    SAFE_CAST(legislacao AS STRING) legislacao,
    SAFE_CAST(shape__length AS STRING) comprimento,
    SAFE_CAST(geometry_wkt AS STRING) geometry_wkt, 
    SAFE.ST_GEOGFROMTEXT(geometry) AS geometry, 
FROM `rj-escritorio-dev.dados_mestres_staging.aeis_bairro_maravilha`

