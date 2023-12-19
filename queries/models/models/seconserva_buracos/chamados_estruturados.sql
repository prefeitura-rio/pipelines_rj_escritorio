WITH waze_inicial AS (
    SELECT
    ts,
    street,
    subtype,
    long,
    lat,
    DATETIME(TIMESTAMP_SECONDS(7*24*60*60 * DIV(UNIX_SECONDS(TIMESTAMP(ts)) + 7*12*60*60, 7*24*60*60))) as ts_trunc, -- aqui 7 dias
    --CONCAT(ts, lat, long) as pre_chave,
    ST_GEOGPOINT(long,lat) as wkt_geometry_waze,
    'waze' as identificador_tabela
    FROM `rj-escritorio-dev.seconserva_buracos.alertas_waze`
    WHERE long is not null and DATE_DIFF(CURRENT_DATE(), ts, DAY) <= 30), -- o que não foi reportado nos últimos 30 dias deve ter sido fechado

    waze_cluster AS (
    SELECT 
    *,
    -- epsilon: The epsilon that specifies the radius, measured in meters, around a core value. 10 20 50
    ST_CLUSTERDBSCAN(wkt_geometry_waze, 30, 1) OVER (PARTITION BY ts_trunc) AS cluster_num, --aqui
    SAFE_CAST(FORMAT_DATE('%s', ts) AS INT64) AS ts_epoch,
    FROM waze_inicial
    ORDER BY cluster_num, ts),

  waze AS (
    SELECT
    identificador_tabela,
    ts_trunc,
    cluster_num,
    TO_BASE64(SHA256(CONCAT(ts_trunc, cluster_num))) as id_cluster_waze,
    ST_CENTROID_AGG(wkt_geometry_waze) centroide_cluster,
    COUNT(*) contagem_buracos
    FROM waze_cluster
    GROUP BY identificador_tabela, ts_trunc, cluster_num
    ORDER BY ts_trunc, cluster_num),
 
    logradouros AS (
    SELECT 
    *,
    geometry wkt_geometry_logradouros,
    --ST_SNAPTOGRID(geometry, 0.00125) grid_logradouros,
    ROW_NUMBER() OVER(PARTITION BY id_logradouro ORDER BY id_trecho DESC) AS ordem_trecho,
    ROW_NUMBER() OVER(PARTITION BY id_trecho ORDER BY nome_completo DESC) AS ordem_nome
    FROM `rj-escritorio-dev.dados_mestres.logradouro`),

    d1746 AS (
    SELECT
    *,
    tipo as tipo_1746,
    ST_GEOGPOINT(longitude, latitude) wkt_geometry_1746,
    lim.bairro as bairro_chamado,
    '1746' as identificador_tabela
    FROM `rj-segovi.administracao_servicos_publicos.chamado_1746` as d
    LEFT JOIN `rj-escritorio-dev.seconserva_buracos.limites_gerencias` as lim
    ON CAST(d.id_bairro as FLOAT64) = CAST(lim.id_bairro as FLOAT64)
    WHERE data_inicio >= '2021-01-01'
      AND id_subtipo = '78'), -- COUNT(*) = 57988
 
    join_waze AS (
    SELECT
    w.*,
    l.*,
    l.nome bairro_logradouros,
    lim.*,
    FROM waze as w
    LEFT JOIN (SELECT * FROM logradouros WHERE ordem_nome = 1) as l -- isso retira trechos duplicados com mesmos id_trechos e geometry mas com nomes diferentes (acentos, typos, etc)
    ON ST_INTERSECTS(w.centroide_cluster, ST_BUFFER(l.wkt_geometry_logradouros, 20)) -- buffer com 20 metros
    LEFT JOIN `rj-escritorio-dev.seconserva_buracos.limites_gerencias` as lim
    ON CAST(l.id_bairro as FLOAT64) = CAST(lim.id_bairro as FLOAT64)
    ),

    waze_one_trecho AS (
      SELECT
        id_cluster_waze,
        id_logradouro,
        MIN(ordem_trecho) primeiro_trecho
      FROM join_waze
      GROUP BY id_cluster_waze, id_logradouro
    ),

    waze_one_street AS (
    SELECT 
    *, 
    ROW_NUMBER() OVER(PARTITION BY id_cluster_waze ORDER BY id_logradouro DESC) AS ordem_logradouros
    FROM (SELECT jw.* FROM join_waze as jw 
          INNER JOIN waze_one_trecho as wot ON jw.id_cluster_waze = wot.id_cluster_waze AND jw.id_logradouro = wot.id_logradouro AND jw.ordem_trecho = wot.primeiro_trecho)
    WHERE bairro_logradouros is not NULL),
    
    join_1746 AS (
    SELECT
    d.*,
    l.*,
    l.nome_completo as nome_logradouro,
    l.nome as bairro_logradouros,
    FROM d1746 as d
    LEFT JOIN (SELECT * FROM logradouros WHERE ordem_nome = 1) as l -- isso retira trechos duplicados com mesmos id_trechos e geometry mas com nomes diferentes (acentos, typos, etc)
    --ON ST_INTERSECTS(d.wkt_geometry_1746 , ST_BUFFER(l.wkt_geometry_logradouros, 20))
    ON LPAD(d.id_logradouro, 6, '0') = l.id_logradouro
    WHERE l.ordem_trecho = 1 or l.ordem_trecho IS NULL), -- pegando apenas um trecho eu elimino linhas duplicadas, mas isso não é um problema pois as informações de localidade
    -- que eu vou passar pro usuario final são o id_logradouro e a localização oriunda do chamado, nenhuma informação sobre o trecho em si. 
    -- Pegando os NULL eu não elimino chamados cujo id_logradouro não foi encontrado na base de logradouros

    ---COMECO UNION
    union_waze_1746 AS(
    SELECT
    id_chamado,
    id_chamado as id_1746,
    NULL as id_waze,
    data_inicio, 
    data_fim,
    status,
    tipo_1746 as tipo,
    subtipo,
    -- ds_chamado as
    nome_unidade_organizacional as unidade_organizacional,
    '1746'  as origem_ocorrencia,
    categoria,
    TRIM(bairro_chamado) as bairro,
    nome_logradouro as logradouro,
    numero_logradouro as endereco_numero,
    -- ds_endereco_cep
    -- ds_endereco_referencia
    -- ds_endereco_complemento
    --no_ra as regiao_administrativa,
    numero_area_planejamento,
    EXTRACT(DATE FROM data_inicio) as data_inicio_data,
    EXTRACT(DATE FROM data_fim) as data_fim_data,
    CASE WHEN data_fim IS NOT NULL
        THEN DATE_DIFF(data_fim, data_inicio, DAY)
        ELSE DATE_DIFF(CURRENT_DATE('-03:00'), CAST(data_inicio AS DATETIME), DAY) END as tempo_de_vida,
        --ELSE DATE_DIFF(CAST('2022-01-30' AS DATETIME), data_inicio, DAY) END as tempo_de_vida,
    EXTRACT(YEAR FROM data_inicio) as data_inicio_ano,
    EXTRACT(QUARTER FROM data_inicio) as data_inicio_trimestre,
    EXTRACT(MONTH FROM data_inicio) as data_inicio_mes,
    EXTRACT(DAY FROM data_inicio) as data_inicio_dia,
    EXTRACT(YEAR FROM data_fim) as data_fim_ano,
    EXTRACT(QUARTER FROM data_fim) as data_fim_trimestre,
    EXTRACT(MONTH FROM data_fim) as data_fim_mes,
    EXTRACT(DAY FROM data_fim) as data_fim_dia,
    'Rio de Janeiro, RJ' cidade, -- colocando na mao mas na verdade deveria verificar se o ponto está ou não na cidade
    CONCAT(nome_logradouro, ' ', IFNULL(CAST(numero_logradouro AS STRING), ''),', ', TRIM(bairro_chamado), ', Rio de Janeiro, RJ') endereco_completo, -- falta o numero de porta, mas nao tenho esse dado,
    hierarquia as hierarquia_viaria,
    ST_Y(wkt_geometry_1746) as latitude, 
    ST_X(wkt_geometry_1746) as longitude,
    NULL as contagem_alertas_cluster_waze,
    numero_gerencia_conservacao_responsavel AS id_gerencia_responsavel
    FROM join_1746

    UNION ALL 

    SELECT
    id_cluster_waze as id_chamado,
    NULL as id_1746,
    id_cluster_waze as id_waze,
    ts_trunc as data_inicio, 
    NULL as data_fim,
    NULL as status,
    NULL as tipo,
    NULL as subtipo,
    -- ds_chamado as
    NULL as unidade_organizacional,
    identificador_tabela as origem_ocorrencia,
    NULL as categoria,
    TRIM(bairro_logradouros) as bairro,
    nome_completo as logradouro,
    NULL as endereco_numero,
    -- ds_endereco_cep
    -- ds_endereco_referencia
    -- ds_endereco_complemento
    --ra as regiao_administrativa,
    numero_area_planejamento,
    EXTRACT(DATE FROM ts_trunc) as data_inicio_data,
    NULL as data_fim_data,
    DATE_DIFF(CURRENT_DATE('-03:00'), CAST(ts_trunc AS DATETIME), DAY) as tempo_de_vida,
    --DATE_DIFF(CAST('2022-01-30' AS DATETIME),ts_trunc, DAY) as tempo_de_vida,
    EXTRACT(YEAR FROM ts_trunc) as data_inicio_ano,
    EXTRACT(QUARTER FROM ts_trunc) as data_inicio_trimestre,
    EXTRACT(MONTH FROM ts_trunc) as data_inicio_mes,
    EXTRACT(DAY FROM ts_trunc) as data_inicio_dia,
    NULL as data_fim_ano,
    NULL as data_fim_trimestre,
    NULL as data_fim_mes,
    NULL as data_fim_dia,
    'Rio de Janeiro, RJ' cidade, -- colocando na mao mas na verdade deveria verificar se o ponto está ou não na cidade
    CONCAT(nome_completo, ', ', TRIM(IFNULL(bairro_logradouros, '')), ', Rio de Janeiro, RJ') endereco_completo, -- falta o numero de porta, mas nao tenho esse dado,
    hierarquia as hierarquia_viaria,
    ST_Y(centroide_cluster) as latitude, 
    ST_X(centroide_cluster) as longitude,
    contagem_buracos as contagem_alertas_cluster_waze,
    numero_gerencia_conservacao_responsavel AS id_gerencia_responsavel
    FROM (SELECT * FROM waze_one_street WHERE ordem_logradouros = 1)
    )
    ---FIM UNION

SELECT 
    *,
   (CASE 
        WHEN tempo_de_vida <= 4 THEN '3 - 1 a 4 dias'
        WHEN tempo_de_vida <= 8 THEN '2 - 5 a 8 dias'
        WHEN tempo_de_vida <= 12 THEN '1 - 9 a 12 dias'
        ELSE '4 - 13 dias ou mais' END) as aging_list,
    (CASE                                                -- origem_ocorrencia = 'waze' OR
        WHEN (8 < tempo_de_vida AND tempo_de_vida <= 12) OR hierarquia_viaria = 'Arterial primária' OR hierarquia_viaria = 'Arterial secundária' 
            THEN 1
        WHEN (4 < tempo_de_vida AND tempo_de_vida <= 8) OR hierarquia_viaria = 'Coletora' 
            THEN 2
        WHEN (0 <= tempo_de_vida AND tempo_de_vida <= 4)
            THEN 3
        ELSE 4 END) as prioridade
FROM union_waze_1746
ORDER BY data_inicio DESC