implemente as metricas abaixo em sql a partir das tabelas source, uma query para cada metrica.
para cada query utilize apenas os campos necessarios da ctes em cada uma das queries.
as metricas devem ser agrupadas por dia

bigquery, preciso criar uma query pra cada metrica, parta da tabela source

buscas
with
    _source_buscas as (
        select
            updated_at,
            nullif(json_value(data, '$.session_id'), "") as session_id,
            safe.parse_date('%d-%m-%Y', json_value(data, '$.date')) as date,
            safe.parse_timestamp(
                '%d-%m-%YT%H:%M:%S',
                concat(json_value(data, '$.date'), 'T', json_value(data, '$.time'))
            ) as timestamp,
            nullif(json_value(data, '$.portal_origem'), "") as portal_origem,
            nullif(json_value(data, '$.query'), "") as query,
            nullif(json_value(data, '$.tipo_dispositivo'), "") as tipo_dispositivo
        from `rj-chatbot.busca.buscas`
    )

select *
from _source_buscas

cliques
with
    _source_cliques as (
        select
            updated_at,

            nullif(json_value(data, '$.session_id'), "") as session_id,
            safe.parse_date('%d-%m-%Y', json_value(data, '$.date')) as date,
            safe.parse_timestamp(
                '%d-%m-%YT%H:%M:%S',
                concat(json_value(data, '$.date'), 'T', json_value(data, '$.time'))
            ) as timestamp,
            nullif(json_value(data, '$.portal_origem'), "") as portal_origem,
            nullif(json_value(data, '$.query'), "") as query,
            nullif(json_value(data, '$.tipo_dispositivo'), "") as tipo_dispositivo,
            cast(nullif(json_value(data, '$.posicao'), "") as int64) as posicao,

            nullif(json_value(data, '$.objeto_clicado.id'), "") as id,
            nullif(json_value(data, '$.objeto_clicado.id_1746'), "") as id_1746,
            nullif(json_value(data, '$.objeto_clicado.id_pref_rio'), "") as id_pref_rio,
            nullif(
                json_value(data, '$.objeto_clicado.id_carioca_digital'), ""
            ) as id_carioca_digital,
            nullif(json_value(data, '$.objeto_clicado.titulo'), "") as titulo,
            cast(
                nullif(json_value(data, '$.objeto_clicado.servico'), "") as bool
            ) as servico,
            nullif(json_value(data, '$.objeto_clicado.descricao'), "") as descricao,
            nullif(json_value(data, '$.objeto_clicado.tipo'), "") as tipo,
            nullif(json_value(data, '$.objeto_clicado.collection'), "") as collection,
            nullif(json_value(data, '$.objeto_clicado.link_acesso'), "") as link_acesso,
            nullif(
                json_value(data, '$.objeto_clicado.link_para_atendimento'), ""
            ) as link_para_atendimento,
            nullif(json_value(data, '$.objeto_clicado.etapas'), "") as etapas,
            nullif(
                json_value(data, '$.objeto_clicado.prazo_esperado'), ""
            ) as prazo_esperado,
            nullif(
                json_value(data, '$.objeto_clicado.informacoes_complementares'), ""
            ) as informacoes_complementares,

            safe.parse_date(
                '%d-%m-%Y', json_value(data, '$.objeto_clicado.ultima_atualizacao')
            ) as data_atualizacao,
            nullif(
                json_value(data, '$.objeto_clicado.orgao_gestor'), ""
            ) as orgao_gestor,
            nullif(
                json_value(data, '$.objeto_clicado.publico_atendido'), ""
            ) as publico_atendido,
            nullif(
                json_value(data, '$.objeto_clicado.custo_do_servico'), ""
            ) as custo_do_servico,
            nullif(
                json_value(data, '$.objeto_clicado.valor_a_ser_pago'), ""
            ) as valor_a_ser_pago,
            nullif(
                json_value(data, '$.objeto_clicado.local_para_atendimento_presencial'),
                ""
            ) as local_para_atendimento_presencial,
            nullif(
                json_value(
                    data,
                    '$.objeto_clicado.informacao_geral_para_atendimento_presencial'
                ),
                ""
            ) as informacao_geral_atendimento_presencial,
            nullif(
                json_value(data, '$.objeto_clicado.tempo_para_atendimento'), ""
            ) as tempo_para_atendimento,
            nullif(
                json_value(data, '$.objeto_clicado.atividades_do_cidadao'), ""
            ) as atividades_do_cidadao,
            nullif(
                json_value(data, '$.objeto_clicado.resultado_da_solicitacao'), ""
            ) as resultado_da_solicitacao,
            nullif(
                json_value(data, '$.objeto_clicado.produtos_do_servico'), ""
            ) as produtos_do_servico,
            cast(
                nullif(
                    json_value(data, '$.objeto_clicado.disponivel_via_aplicativo'), ""
                ) as bool
            ) as disponivel_via_aplicativo,
            cast(
                nullif(
                    json_value(data, '$.objeto_clicado.servico_em_manutencao'), ""
                ) as bool
            ) as servico_em_manutencao,

            nullif(
                json_value(data, '$.objeto_clicado.category.macro'), ""
            ) as category_macro,
            nullif(
                json_value(data, '$.objeto_clicado.category.micro'), ""
            ) as category_micro,
            nullif(
                json_value(data, '$.objeto_clicado.category.specific'), ""
            ) as category_specific

        from `rj-chatbot.busca.cliques`
    )

select *
from _source_cliques


Engajamento & Comportamento
Taxa de Retorno à Busca: % de usuários que voltam a pesquisar após clicar em um resultado (indica insatisfação com o resultado clicado).
Como medir: Comparar sessões com múltiplas buscas após um clique

Profundidade da Sessão: Número médio de buscas por sessão (reflete a necessidade de refinamento da query).
Como medir: Contar queries únicas por session_id.

Taxa de Abandono por Portal: % de buscas sem cliques

Relevância & Qualidade dos Resultados
Assuntos mais buscados: queries mais buscadas pelos usuários.

Mean Reciprocal Rank (MRR): Média do inverso da posição do primeiro resultado clicado.
Exemplo: Se o primeiro clique for na posição 2, MRR = 1/2.
Indica qualidade do ranking.

Diversidade de Cliques: Número médio de resultados únicos clicados por query (evita "monocultura" de cliques no topo).
Como medir: Contar objeto clicado distintos por query.

Comportamento por Dispositivo/Portal
CTR por Dispositivo: Comparar click-through rate entre mobile/desktop.
Exemplo: Usuários mobile podem ter menor CTR se a UI for menos intuitiva.