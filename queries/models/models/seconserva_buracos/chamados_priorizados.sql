WITH chamados_fechados AS (
SELECT 
todos.*,
(CASE 
    WHEN (fechados.id_chamado IS NOT NULL and todos.origem_ocorrencia = 'waze')  THEN 'Fechado'
    WHEN todos.data_fim IS NOT NULL THEN 'Fechado'
    ELSE 'Aberto' END) AS status_retorno_fechamento
FROM (SELECT * FROM `rj-escritorio-dev.seconserva_buracos.retorno_chamados_fechados` WHERE id_gerencia IS NOT NULL) AS fechados
RIGHT JOIN `rj-escritorio-dev.seconserva_buracos.chamados_estruturados` AS todos
    ON fechados.id_chamado = todos.id_chamado)

SELECT 
* 
FROM chamados_fechados 
--WHERE data_fim IS NULL
--    OR status_retorno_fechamento = 'Aberto'
