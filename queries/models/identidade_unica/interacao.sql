-- educacao
(
  SELECT
    DISTINCT
    SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(cpf), r'\.0$', '') AS STRING)) AS id_hash,
    "Participação no sistema escolar" AS tipo,
    situacao as status,
    CAST(data_particao AS datetime) AS data_status
  FROM
    `rj-sme.educacao_basica_staging.aluno`
)

UNION ALL

(
  SELECT
    DISTINCT
    SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(cpf_cnpj), r'\.0$', '') AS STRING)) AS id_hash,
    "Dívida ativa" AS tipo,
    "Inscrito" as status,
    DATE_TRUNC(data_ultima_atualizacao, day) AS data_status
  FROM
    `rj-pgm.adm_financas_divida_ativa.inscritos_divida_ativa`
  WHERE tipo_documento = "CPF"
)

UNION ALL

(

  WITH filter_table AS (
    SELECT
      DISTINCT
      SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(func.id_cpf), r'\.0$', '') AS STRING)) AS id_hash,
      CONCAT("Licença afastamento servidor por ", afast.id_afastamento) AS tipo, -- TODO: trocar id_afastamento por valor correspondente, aguardando tabela 
      data_inicio,
      data_final
    FROM `rj-smfp.recursos_humanos_ergon.funcionario` func
    inner join `rj-smfp.recursos_humanos_ergon.licenca_afastamento` afast ON afast.id_vinculo = func.id_vinculo AND afast.data_particao >= "2024-01-01" -- TODO: ampliar filtro de data
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

(
  -- WITH 
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
      CONCAT("Licença afastamento servidor por ", afast.id_afastamento) AS tipo, -- TODO: trocar id_afastamento por valor correspondente, aguardando tabela 
      data_inicio,
      data_final
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

(