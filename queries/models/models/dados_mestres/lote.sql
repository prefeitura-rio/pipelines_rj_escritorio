
SELECT 
    SAFE_CAST(cod_lote AS STRING) id_lote, 
    SAFE_CAST(cod_quadra AS STRING) id_quadra, 
    SAFE_CAST(cod_np AS STRING) id_numero_porta, 
    SAFE_CAST(cod_trecho AS STRING) id_trecho,  
    SAFE_CAST(obs AS STRING) observacao,
    SAFE_CAST(shape__area AS STRING) area_poligono, 
    SAFE_CAST(shape__length AS STRING) comprimento_poligono, 
    SAFE_CAST(geometry_wkt AS STRING) geometria_wkt, 
    SAFE.ST_GEOGFROMTEXT(geometry) AS geometry,
FROM `rj-escritorio-dev.dados_mestres_staging.lote`
