-- CREATE OR REPLACE TABLE `rj-escritorio-dev.identidade_unica.interacao` AS

-- (  -- Dados alunos escolas
--   SELECT
--     DISTINCT
--     SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(cpf), r'\.0$', '') AS STRING)) AS id_hash,
--     "Participação no sistema escolar" AS tipo,
--     situacao as status,
--     CAST(data_particao AS datetime) AS data_status
--   FROM
--     `rj-sme.educacao_basica_staging.aluno_historico`
-- )

-- UNION ALL

( -- CPFs inscritos na dívida ativa
  SELECT
    DISTINCT
    SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(cpf_cnpj), r'\.0$', '') AS STRING)) AS id_hash,
    "Dívida ativa" AS tipo,
    "Inscrito" as status,
    CAST(data_ultima_atualizacao AS DATETIME) AS data_status
  FROM
    `rj-pgm.adm_financas_divida_ativa.inscritos_divida_ativa`
  WHERE tipo_documento = "CPF"
)

UNION ALL

(-- Funcionários PCRJ COMLURB - Afastamento
WITH
  -- dim AS (
  --   SELECT
  --     DISTINCT
  --     id_cpf AS cpf,
  --     SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(id_cpf), r'\.0$', '') AS STRING)) AS id_hash,
  --     id_funcionario
  --   FROM `rj-smfp.recursos_humanos_ergon_comlurb.fita_banco`
  -- ),

  filter_table AS (
    SELECT
      DISTINCT
      dim.id_hash,
      "SERVIDOR - LICENCA AFASTAMENTO" AS tipo,
      CAST(data_inicio AS DATETIME) data_inicio,
      CAST(data_fim AS DATETIME) data_fim
    FROM `rj-smfp.recursos_humanos_ergon_comlurb.funcionario` func
    -- INNER JOIN dim on func.id_cpf = dim.cpf
    INNER JOIN {{ ref('dim_funcionario_cpf') }} on func.id_cpf = dim.cpf
    inner join `rj-smfp.recursos_humanos_ergon_comlurb.licenca_afastamento` afast ON afast.id_funcionario = dim.id_funcionario
  )

  (
    SELECT
      DISTINCT
      id_hash,
      tipo,
      "Início" as status,
      data_inicio AS data_status
    FROM filter_table
  )
  UNION ALL
  (
    SELECT
      DISTINCT
      id_hash,
      tipo,
      "Final" as status,
      data_fim AS data_status
    FROM filter_table
    WHERE data_fim IS NOT NULL
  )
  -- adicionar troca de cargo e área? no afastamento já tem aposentadoria?

)

UNION ALL

( -- Funcionários PCRJ exceto COMLURB: Afastamento
  WITH
  -- dim AS (
  --   SELECT
  --     DISTINCT
  --     cpf,
  --     SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(cpf), r'\.0$', '') AS STRING)) AS id_hash,
  --     numfunc AS id_funcionario -- TODO: confirmar se numfunc é o id_funcionario
  --   FROM `rj-smfp.recursos_humanos_ergon.fita_banco`
  -- ),

  filter_table AS (
    SELECT
      DISTINCT
      dim.id_hash,
      CONCAT("SERVIDOR - LICENCA AFASTAMENTO POR ", afast.id_afastamento) AS tipo, -- TODO: trocar id_afastamento por valor correspondente, aguardando tabela
      CAST(data_inicio AS DATETIME) data_inicio,
      CAST(data_final AS DATETIME) data_final
    FROM `rj-smfp.recursos_humanos_ergon.funcionario` func
    -- INNER JOIN dim on func.id_cpf = dim.cpf
    INNER JOIN {{ ref('dim_funcionario_cpf') }} on func.id_cpf = dim.cpf
    inner join `rj-smfp.recursos_humanos_ergon.licenca_afastamento` afast ON afast.id_funcionario = dim.id_funcionario AND afast.data_particao >= "2024-01-01" -- TODO: ampliar filtro de data
  )

  (
    SELECT
      DISTINCT
      id_hash,
      tipo,
      "Início" as status,
      data_inicio AS data_status
    FROM filter_table
  )
  UNION ALL
  (
    SELECT
      DISTINCT
      id_hash,
      tipo,
      "Final" as status,
      data_final AS data_status
    FROM filter_table
    WHERE data_final IS NOT NULL
  )
  -- adicionar troca de cargo e área? no afastamento já tem aposentadoria?


)

UNION ALL

( -- Funcionários PCRJ exceto COMLURB: Nomeação, vacância e aposentadoria
  WITH
  -- dim AS (
  --   SELECT
  --     DISTINCT
  --     cpf,
  --     SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(cpf), r'\.0$', '') AS STRING)) AS id_hash,
  --     numfunc AS id_funcionario -- TODO: confirmar se numfunc é o id_funcionario
  --   FROM `rj-smfp.recursos_humanos_ergon.fita_banco`
  -- ),

  filter_table AS (
    SELECT
      DISTINCT
      dim.id_hash,
      CONCAT("SERVIDOR - TIPO DE VINCULO ", vinculo.tipo) AS tipo,
      CAST(data_nomeacao AS DATETIME) data_nomeacao,
      CAST(data_posse AS DATETIME) data_posse,
      CAST(data_exercicio AS DATETIME) data_exercicio,
      CAST(data_inicio_contrato AS DATETIME) data_inicio_contrato,
      CAST(data_fim_contrato AS DATETIME) data_fim_contrato,
      CAST(data_prorrogacao_contrato AS DATETIME) data_prorrogacao_contrato,
      CAST(data_aposentadoria AS DATETIME) data_aposentadoria,
      CAST(data_vacancia AS DATETIME) data_vacancia,
      CAST(data_inicio_cessao AS DATETIME) data_inicio_cessao,
      CAST(data_fim_cessao AS DATETIME) data_fim_cessao
    FROM `rj-smfp.recursos_humanos_ergon.funcionario` func
    -- INNER JOIN dim on func.id_cpf = dim.cpf
    INNER JOIN {{ ref('dim_funcionario_cpf') }} dim on func.id_cpf = dim.cpf AND dim.comlurb = 0
    INNER JOIN `rj-smfp.recursos_humanos_ergon.vinculo` vinculo ON vinculo.id_funcionario = dim.id_funcionario
  )

  (
    -- Subconsulta para datas de nomeação
    SELECT
      id_hash,
      CONCAT(tipo, " NOMEACAO") tipo,
      'Inicio' AS status,
      data_nomeacao AS data_status
    FROM filter_table
    WHERE data_nomeacao IS NOT NULL
    UNION ALL
    -- Subconsulta para datas de posse
    SELECT
      id_hash,
      CONCAT(tipo, " POSSE") tipo,
      'Inicio' AS status,
      data_posse AS data_status
    FROM filter_table
    WHERE data_posse IS NOT NULL
    UNION ALL
    -- Subconsulta para datas de exercicio
    SELECT
      id_hash,
      CONCAT(tipo, " EXERCICIO") tipo,
      'Inicio' AS status,
      data_exercicio AS data_status
    FROM filter_table
    WHERE data_exercicio IS NOT NULL
    UNION ALL
    -- Subconsulta para datas de aposentadoria
    SELECT
      id_hash,
      CONCAT(tipo, " APOSENTADORIA") tipo,
      'Inicio' AS status,
      data_aposentadoria AS data_status,
    FROM filter_table
    WHERE data_aposentadoria IS NOT NULL
    UNION ALL
    -- Subconsulta para datas de vacância
    SELECT
      id_hash,
      CONCAT(tipo, " VACANCIA") tipo,
      'Final' AS status,
      data_vacancia AS data_status,
    FROM filter_table
    WHERE data_vacancia IS NOT NULL
    UNION ALL
    -- Subconsulta para datas de início de cessão
    SELECT
      id_hash,
      CONCAT(tipo, " CESSAO") tipo,
      'Inicio' AS status,
      data_inicio_cessao AS data_status,
    FROM filter_table
    WHERE data_inicio_cessao IS NOT NULL
    UNION ALL
    -- Subconsulta para datas de FIM de cessão
    SELECT
      id_hash,
      CONCAT(tipo, " CESSAO") tipo,
      'Final' AS status,
      data_fim_cessao AS data_status,
    FROM filter_table
    WHERE data_fim_cessao IS NOT NULL
    UNION ALL
    -- Subconsulta para datas de início de contrato
    SELECT
      id_hash,
      CONCAT(tipo, " CONTRATO") tipo,
      'Inicio' AS status,
      data_inicio_contrato AS data_status,
    FROM filter_table
    WHERE data_inicio_contrato IS NOT NULL
    UNION ALL
    -- Subconsulta para datas de fim de contrato
    SELECT
      id_hash,
      CONCAT(tipo, " CONTRATO") tipo,
      'Final' AS status,
      data_fim_contrato AS data_status,
    FROM filter_table
    WHERE data_fim_contrato IS NOT NULL
    UNION ALL
    -- Subconsulta para datas de prorrogação de contrato
    SELECT
      id_hash,
      CONCAT(tipo, " CONTRATO") tipo,
      'PRORROGACAO' AS status,
      data_prorrogacao_contrato AS data_status,
    FROM filter_table
    WHERE data_prorrogacao_contrato IS NOT NULL
)


)

UNION ALL

(-- Funcionários PCRJ COMLURB: Nomeação, vacância e aposentadoria
  WITH
-- dim AS (
--   SELECT
--     DISTINCT
--     id_cpf as cpf,
--     SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(id_cpf), r'\.0$', '') AS STRING)) AS id_hash,
--     id_funcionario, -- TODO: confirmar se numfunc é o id_funcionario,
--     1 AS comlurb
--   FROM `rj-smfp.recursos_humanos_ergon_comlurb.fita_banco`
-- ),

filter_table AS (
  SELECT
    DISTINCT
    dim.id_hash,
    CONCAT("SERVIDOR - TIPO DE VINCULO ", vinculo.categoria) AS tipo,
    CAST(data_nomeacao AS DATETIME) data_nomeacao,
    CAST(data_posse AS DATETIME) data_posse,
    CAST(data_inicio_exercicio AS DATETIME) data_inicio_exercicio,
    CAST(data_inicio_contrato AS DATETIME) data_inicio_contrato,
    CAST(data_fim_contrato AS DATETIME) data_fim_contrato,
    CAST(data_prorrogacao_contrato AS DATETIME) data_prorrogacao_contrato,
    CAST(data_aposentadoria AS DATETIME) data_aposentadoria,
    CAST(data_vacancia AS DATETIME) data_vacancia,
    CAST(data_inicio_cessao AS DATETIME) data_inicio_cessao,
    CAST(data_fim_cessao AS DATETIME) data_fim_cessao
  FROM `rj-smfp.recursos_humanos_ergon_comlurb.funcionario` func
  -- INNER JOIN dim on func.id_cpf = dim.cpf AND dim.comlurb = 1
  INNER JOIN {{ ref('dim_funcionario_cpf') }} dim on func.id_cpf = dim.cpf AND dim.comlurb = 1
  inner join `rj-smfp.recursos_humanos_ergon_comlurb.vinculo` vinculo ON vinculo.id_funcionario = dim.id_funcionario
)

(
  -- Subconsulta para datas de nomeação
  SELECT
    id_hash,
    CONCAT(tipo, " NOMEACAO") tipo,
    'Inicio' AS status,
    data_nomeacao AS data_status
  FROM filter_table
  WHERE data_nomeacao IS NOT NULL
  UNION ALL
  -- Subconsulta para datas de posse
  SELECT
    id_hash,
    CONCAT(tipo, " POSSE") tipo,
    'Inicio' AS status,
    data_posse AS data_status
  FROM filter_table
  WHERE data_posse IS NOT NULL
  UNION ALL
  -- Subconsulta para datas de exercicio
  SELECT
    id_hash,
    CONCAT(tipo, " EXERCICIO") tipo,
    'Inicio' AS status,
    data_inicio_exercicio AS data_status
  FROM filter_table
  WHERE data_inicio_exercicio IS NOT NULL
  UNION ALL
  -- Subconsulta para datas de aposentadoria
  SELECT
    id_hash,
    CONCAT(tipo, " APOSENTADORIA") tipo,
    'Inicio' AS status,
    data_aposentadoria AS data_status,
  FROM filter_table
  WHERE data_aposentadoria IS NOT NULL
  UNION ALL
  -- Subconsulta para datas de vacância
  SELECT
    id_hash,
    CONCAT(tipo, " VACANCIA") tipo,
    'Final' AS status,
    data_vacancia AS data_status,
  FROM filter_table
  WHERE data_vacancia IS NOT NULL
  UNION ALL
  -- Subconsulta para datas de início de cessão
  SELECT
    id_hash,
    CONCAT(tipo, " CESSAO") tipo,
    'Inicio' AS status,
    data_inicio_cessao AS data_status,
  FROM filter_table
  WHERE data_inicio_cessao IS NOT NULL
  UNION ALL
  -- Subconsulta para datas de fim de cessão
  SELECT
    id_hash,
    CONCAT(tipo, " CESSAO") tipo,
    'Final' AS status,
    data_fim_cessao AS data_status,
  FROM filter_table
  WHERE data_fim_cessao IS NOT NULL
  UNION ALL
  -- Subconsulta para datas de início de contrato
  SELECT
    id_hash,
    CONCAT(tipo, " CONTRATO") tipo,
    'Inicio' AS status,
    data_inicio_contrato AS data_status,
  FROM filter_table
  WHERE data_inicio_contrato IS NOT NULL
  UNION ALL
  -- Subconsulta para datas de FIM de contrato
  SELECT
    id_hash,
    CONCAT(tipo, " CONTRATO") tipo,
    'Final' AS status,
    data_fim_contrato AS data_status,
  FROM filter_table
  WHERE data_fim_contrato IS NOT NULL
  UNION ALL
  -- Subconsulta para datas de prorrogação de contrato
  SELECT
    id_hash,
    CONCAT(tipo, " CONTRATO") tipo,
    'PRORROGACAO' AS status,
    data_prorrogacao_contrato AS data_status,
  FROM filter_table
  WHERE data_prorrogacao_contrato IS NOT NULL
)

)

UNION ALL

(
  WITH filter_table AS (
  SELECT
    DISTINCT
    SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(cnpj_cpf_favorecido), r'\.0$', '') AS STRING)) AS id_hash,
    UPPER(tipo_instrumento) tipo_instrumento,
    CAST(data_inicio_prevista AS DATETIME) data_inicio_prevista,
    CAST(data_fim_prevista AS DATETIME) data_fim_prevista,
    CAST(data_assinatura AS DATETIME) data_assinatura
  FROM `rj-smfp.adm_instrumentos_firmados.instrumento_firmado`
  WHERE tipo_favorecido = "Pessoa Física"
)
    -- Subconsulta para datas de início previsto
    SELECT
      id_hash,
      CONCAT("FIRMADO ", tipo_instrumento) tipo,
      'Inicio' AS status,
      data_inicio_prevista AS data_status
    FROM filter_table
    WHERE data_inicio_prevista IS NOT NULL
    UNION ALL
    -- Subconsulta para datas de fim previsto
    SELECT
      id_hash,
      CONCAT("FIRMADO ", tipo_instrumento) tipo,
      'Final' AS status,
      data_fim_prevista AS data_status
    FROM filter_table
    WHERE data_fim_prevista IS NOT NULL
)

UNION ALL

(
  WITH filter_table AS (
    SELECT
      DISTINCT
      SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(cpf_cnpj), r'\.0$', '') AS STRING)) AS id_hash,
      CONCAT("SANCAO POR ", UPPER(descricao_sancao)) tipo,
      CAST(data_sancao AS DATETIME) data_sancao,
      CAST(data_extincao_sancao AS DATETIME) data_extincao_sancao,
    FROM `rj-smfp.adm_orcamento_sigma.sancao_fornecedor`
    WHERE tipo_documento = "CPF"
 )

(
  -- Subconsulta para datas de início
  SELECT
    id_hash,
    tipo,
    'Inicio' AS status,
    data_sancao AS data_status
  FROM filter_table
  WHERE data_sancao IS NOT NULL
  UNION ALL
  -- Subconsulta para datas de fim previsto
  SELECT
    id_hash,
    tipo,
    'Final' AS status,
    data_extincao_sancao AS data_status
  FROM filter_table
  WHERE data_extincao_sancao IS NOT NULL
)
)

UNION ALL

(
  SELECT
      SHA512(SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(cnpj_fornecedor), r'\.0$', ''), r'^0+', '') AS STRING)) AS id_hash,
      CONCAT("COMPRA DE MATERIAL") tipo,
      'Início' AS status,
      CAST(
        CONCAT(
          SUBSTRING(data_nota_fiscal, 1, 4),
          '-',
          SUBSTRING(data_nota_fiscal, 5, 2),
          '-',
          SUBSTRING(data_nota_fiscal, 7, 2),
          ' 00:00:00'
        ) AS DATETIME
    ) AS data_status
    FROM `rj-smfp.compras_materiais_servicos_sigma_staging.movimentacao` mov
    LEFT JOIN `rj-smfp.compras_materiais_servicos_sigma_staging.fornecedor_sem_vinculo` fornsv on fornsv.cpf_cnpj = mov.cnpj_fornecedor
      AND fornsv.tipo_cpf_cnpj = "F"
    LEFT JOIN `rj-smfp.compras_materiais_servicos_sigma_staging.fornecedor` forn on forn.cpf_cnpj = mov.cnpj_fornecedor
      AND forn.tipo_cpf_cnpj = "F"
    WHERE data_nota_fiscal IS NOT NULL 
      AND cd_movimentacao = "2"
      AND (forn.tipo_cpf_cnpj IS NOT NULL OR fornsv.tipo_cpf_cnpj IS NOT NULL)
)