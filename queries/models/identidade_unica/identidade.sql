-- CREATE OR REPLACE TABLE `rj-escritorio-dev.identidade_unica.identidade` AS
( -- Dados alunos escolas
  WITH UltimaDataPorCPF AS (
    SELECT
      cpf,
      MAX(data_particao) AS max_data_particao
    FROM `rj-sme.educacao_basica_staging.aluno_historico`
    GROUP BY cpf
  )

  SELECT
    a.cpf,
    SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(a.cpf), r'\.0$', '') AS STRING)) AS id_hash,
    UPPER(a.nome) AS nome,
    a.sexo genero, -- "Feminino"/"Masculino"
    CAST(a.datanascimento AS DATE) AS data_nascimento,
    UPPER(a.naturalidade) naturalidade,
    UPPER(a.nacionalidade) nacionalidade,
    UPPER(a.raca_cor) raca_cor, -- Parda/Preta/Branca/Amarela/Indígena/Sem Informação/Não declarada/
    UPPER(REGEXP_REPLACE(TRIM(a.deficiencia), r'\*', '')) deficiencia, -- Sem Deficiência	 Deficiência intelectual	 Deficiência física	  Transtorno do espectro autista	*Deficiência múltipla	  Altas habilidades/superdotação	 Deficiência auditiva	 Visão monocular	 Baixa visão	 Surdez	 Cegueira	 Surdocegueira	TGD/Transtornos Invasivos sem outra especificação
    a.bolsa_familia,
    UPPER(a.endereco) endereco,
    UPPER(a.bairro) bairro,
    CAST(NULL AS STRING) AS municipio,
    a.cep
  FROM `rj-sme.educacao_basica_staging.aluno_historico` AS a
  INNER JOIN UltimaDataPorCPF AS b
  ON a.cpf = b.cpf AND a.data_particao = b.max_data_particao
)

UNION ALL

( -- CPFs inscritos na dívida ativa
  WITH UltimaDataPorCPF AS (
    SELECT
        DISTINCT
        SAFE_CAST(REGEXP_REPLACE(TRIM(da.cpf_cnpj), r'\.0$', '') AS STRING) AS cpf,
        SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(da.cpf_cnpj), r'\.0$', '') AS STRING)) AS id_hash,
        UPPER(nome) AS nome,
        CAST(NULL AS STRING) AS genero,
        CAST(NULL AS DATE) AS data_nascimento,
        CAST(NULL AS STRING) AS naturalidade,
        CAST(NULL AS STRING) AS nacionalidade,
        CAST(NULL AS STRING) AS raca_cor,
        CAST(NULL AS STRING) AS deficiencia,
        CAST(NULL AS STRING) AS bolsa_familia,
        CAST(NULL AS STRING) AS endereco,
        CAST(NULL AS STRING) AS bairro,
        CAST(NULL AS STRING) AS municipio,
        CAST(NULL AS STRING) AS cep,
        ROW_NUMBER() OVER (PARTITION BY cpf_cnpj ORDER BY data_ultima_atualizacao DESC) AS rownumber
    FROM
        `rj-pgm.adm_financas_divida_ativa.inscritos_divida_ativa` da
    WHERE tipo_documento = "CPF"
        AND nome NOT LIKE "ESPOLIO%"
        AND nome NOT LIKE "ESPÓLIO%"
  )

    SELECT * EXCEPT (rownumber)
    FROM UltimaDataPorCPF
    WHERE rownumber = 1
)

UNION ALL

( -- Funcionários PCRJ exceto COMLURB
  SELECT
    DISTINCT
    SAFE_CAST(REGEXP_REPLACE(TRIM(id_cpf), r'\.0$', '') AS STRING) as cpf,
    SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(id_cpf), r'\.0$', '') AS STRING)) AS id_hash,
    UPPER(nome) AS nome,
    UPPER(CASE WHEN sexo="F" THEN "Feminino" WHEN sexo="M" THEN "Masculino" ELSE "Outro" END) AS genero,
    data_nascimento,
    UPPER(municipio_nascimento) AS naturalidade,
    UPPER(nacionalidade) nacionalidade,  -- TODO: outra tabela
    CAST(raca_cor AS STRING) AS raca_cor, -- TODO: outra tabela
    CASE
      WHEN UPPER(deficiente) = "N" THEN "Náo possui"
      WHEN deficiente IS NOT NULL THEN
        CASE
            WHEN CONCAT(
                IFNULL(deficiente_auditivo, ''),
                IFNULL(deficiente_fisico, ''),
                IFNULL(deficiente_visual, ''),
                IFNULL(deficiente_mental, ''),
                IFNULL(deficiente_intelectual, '')
            ) = '' THEN "Não especificado"
            WHEN UPPER(CONCAT(
                IFNULL(deficiente_auditivo, ''),
                IFNULL(deficiente_fisico, ''),
                IFNULL(deficiente_visual, ''),
                IFNULL(deficiente_mental, ''),
                IFNULL(deficiente_intelectual, '')
            )) != "S" THEN "Deficiência múltipla"
            WHEN deficiente_auditivo IS NOT NULL THEN "Deficiência auditiva"
            WHEN (deficiente_intelectual IS NOT NULL) OR (deficiente_mental IS NOT NULL) THEN "Deficiência intelectual"
            WHEN deficiente_fisico IS NOT NULL THEN "Deficiência física"
            WHEN deficiente_visual IS NOT NULL THEN "Deficiência visual"
        END
        ELSE NULL
    END AS deficiencia, -- TODO: Adicionar o tipo de deficiencia nesse case when
    CAST(NULL AS STRING) AS bolsa_familia,
    UPPER(CONCAT(tipo_logradouro, logradouro, numero_porta, complemento_numero_porta)) AS endereco,
    UPPER(bairro) AS bairro,
    UPPER(municipio) AS municipio,
    cep
  FROM
    `rj-smfp.recursos_humanos_ergon.funcionario`
)

UNION ALL

( -- Funcionários PCRJ COMLURB
  SELECT
    DISTINCT
    SAFE_CAST(REGEXP_REPLACE(TRIM(id_cpf), r'\.0$', '') AS STRING) as cpf,
    SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(id_cpf), r'\.0$', '') AS STRING)) AS id_hash,
    UPPER(nome) AS nome,
    UPPER(CASE WHEN sexo="F" THEN "Feminino" WHEN sexo="M" THEN "Masculino" ELSE "Outro" END) AS genero,
    data_nascimento,
    UPPER(municipio_nascimento) AS naturalidade,
    UPPER(nacionalidade) nacionalidade,  -- TODO: outra tabela
    CAST(raca_cor AS STRING) AS raca_cor, -- TODO: outra tabela
    CASE
      WHEN UPPER(deficiente) = "N" THEN "Náo possui"
      WHEN (deficiente IS NOT NULL OR UPPER(deficiente) = "S") THEN
        CASE
            WHEN CONCAT(
                IFNULL(deficiencia_auditiva, ''),
                IFNULL(deficiencia_fisica, ''),
                IFNULL(deficiencia_visual, ''),
                IFNULL(deficiencia_mental, ''),
                IFNULL(deficiencia_intelectual, '')
            ) = '' THEN "Não especificado"
            WHEN UPPER(CONCAT(
                IFNULL(deficiencia_auditiva, ''),
                IFNULL(deficiencia_fisica, ''),
                IFNULL(deficiencia_visual, ''),
                IFNULL(deficiencia_mental, ''),
                IFNULL(deficiencia_intelectual, '')
            )) != "S" THEN "Deficiência múltipla"
            WHEN deficiencia_auditiva IS NOT NULL THEN "Deficiência auditiva"
            WHEN (deficiencia_intelectual IS NOT NULL) OR (deficiencia_mental IS NOT NULL) THEN "Deficiência intelectual"
            WHEN deficiencia_fisica IS NOT NULL THEN "Deficiência física"
            WHEN deficiencia_visual IS NOT NULL THEN "Deficiência visual"
        END
        ELSE NULL
    END AS deficiencia, -- TODO: Adicionar o tipo de deficiencia nesse case when
    CAST(NULL AS STRING) AS bolsa_familia,
    UPPER(CONCAT(tipo_logradouro, logradouro, numero_porta, complemento_numero_porta)) AS endereco,
    UPPER(bairro) AS bairro,
    UPPER(municipio) AS municipio,
    cep
  FROM
    `rj-smfp.recursos_humanos_ergon_comlurb.funcionario`
)

UNION ALL

( -- instrumentos firmados
  SELECT 
    DISTINCT
    SAFE_CAST(REGEXP_REPLACE(TRIM(cnpj_cpf_favorecido), r'\.0$', '') AS STRING) AS cpf,
    SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(cnpj_cpf_favorecido), r'\.0$', '') AS STRING)) AS id_hash,
    UPPER(nome_favorecido) nome,
    CAST(NULL AS STRING) AS genero,
    CAST(NULL AS DATE) AS data_nascimento,
    CAST(NULL AS STRING) AS naturalidade,
    CAST(NULL AS STRING) AS nacionalidade,
    CAST(NULL AS STRING) AS raca_cor,
    CAST(NULL AS STRING) AS deficiencia,
    CAST(NULL AS STRING) AS bolsa_familia,
    CAST(NULL AS STRING) AS endereco,
    CAST(NULL AS STRING) AS bairro,
    CAST(NULL AS STRING) AS municipio,
    CAST(NULL AS STRING) AS cep,
  FROM `rj-smfp.adm_instrumentos_firmados.instrumento_firmado`
  WHERE tipo_favorecido = "Pessoa Física"
)

UNION ALL

(
  SELECT
    DISTINCT
    SAFE_CAST(REGEXP_REPLACE(TRIM(cpf_cnpj), r'\.0$', '') AS STRING) AS cpf,
    SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(cpf_cnpj), r'\.0$', '') AS STRING)) AS id_hash,
    UPPER(razao_social) nome,
    CAST(NULL AS STRING) AS genero,
    CAST(NULL AS DATE) AS data_nascimento,
    CAST(NULL AS STRING) AS naturalidade,
    CAST(NULL AS STRING) AS nacionalidade,
    CAST(NULL AS STRING) AS raca_cor,
    CAST(NULL AS STRING) AS deficiencia,
    CAST(NULL AS STRING) AS bolsa_familia,
    CAST(NULL AS STRING) AS endereco,
    CAST(NULL AS STRING) AS bairro,
    CAST(NULL AS STRING) AS municipio,
    CAST(NULL AS STRING) AS cep,
  FROM `rj-smfp.adm_orcamento_sigma.sancao_fornecedor`
  WHERE tipo_documento = "CPF"
)

UNION ALL 

(
  WITH UltimaDataPorCPF AS (
    SELECT
      cpf_cnpj,
      MAX(data_ultima_atualizacao) AS max_data_particao
    FROM `rj-smfp.compras_materiais_servicos_sigma_staging.fornecedor_sem_vinculo`
    WHERE tipo_cpf_cnpj = "F"
    GROUP BY cpf_cnpj
  )
  SELECT
    DISTINCT
    SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(forn.cpf_cnpj), r'\.0$', ''), r'^0+', '') AS STRING) AS cpf,
    SHA512(SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(forn.cpf_cnpj), r'\.0$', ''), r'^0+', '') AS STRING)) AS id_hash,
    UPPER(nome) nome,
    CAST(NULL AS STRING) AS genero,
    CAST(NULL AS DATE) AS data_nascimento,
    CAST(NULL AS STRING) AS naturalidade,
    CAST(NULL AS STRING) AS nacionalidade,
    CAST(NULL AS STRING) AS raca_cor,
    CAST(NULL AS STRING) AS deficiencia,
    CAST(NULL AS STRING) AS bolsa_familia,
    CAST(CONCAT(logradouro, numero_porta, complemento) AS STRING) AS endereco,
    bairro AS bairro,
    municipio AS municipio,
    cep AS cep,
  FROM `rj-smfp.compras_materiais_servicos_sigma_staging.fornecedor_sem_vinculo` forn
  INNER JOIN UltimaDataPorCPF ON UltimaDataPorCPF.max_data_particao = forn.data_ultima_atualizacao
)

UNION ALL

(
  WITH UltimaDataPorCPF AS (
    SELECT
      cpf_cnpj,
      MAX(data_ultima_atualizacao) AS max_data_particao
    FROM `rj-smfp.compras_materiais_servicos_sigma_staging.fornecedor`
    WHERE tipo_cpf_cnpj = "F"
    GROUP BY cpf_cnpj
  )
  SELECT
    DISTINCT
    SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(forn.cpf_cnpj), r'\.0$', ''), r'^0+', '') AS STRING) AS cpf,
    SHA512(SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(forn.cpf_cnpj), r'\.0$', ''), r'^0+', '') AS STRING)) AS id_hash,
    UPPER(razao_social) nome,
    CAST(NULL AS STRING) AS genero,
    CAST(NULL AS DATE) AS data_nascimento,
    CAST(NULL AS STRING) AS naturalidade,
    CAST(NULL AS STRING) AS nacionalidade,
    CAST(NULL AS STRING) AS raca_cor,
    CAST(NULL AS STRING) AS deficiencia,
    CAST(NULL AS STRING) AS bolsa_familia,
    CAST(CONCAT(logradouro, numero_porta, complemento) AS STRING) AS endereco,
    bairro AS bairro,
    municipio AS municipio,
    cep AS cep,
  FROM `rj-smfp.compras_materiais_servicos_sigma_staging.fornecedor` forn
  INNER JOIN UltimaDataPorCPF ON UltimaDataPorCPF.max_data_particao = forn.data_ultima_atualizacao
)

UNION ALL

(
  WITH union_tables AS
    (SELECT
      DISTINCT
      cpf,
      SHA512(SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(cpf), r'\.0$', ''), r'^0+', '') AS STRING)) AS id_hash,
    FROM `rj-chatbot-dev.dialogflowcx.fim_conversas`
    WHERE cpf IS NOT NULL

    UNION ALL

    SELECT
      DISTINCT
      cpf,
      SHA512(SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(cpf), r'\.0$', ''), r'^0+', '') AS STRING)) AS id_hash,
    FROM `rj-chatbot-dev.dialogflowcx.fim_conversas_da`
    WHERE cpf IS NOT NULL)

  SELECT 
    DISTINCT
      cpf,
      id_hash,
      CAST(NULL AS STRING) nome,
      CAST(NULL AS STRING) AS genero,
      CAST(NULL AS DATE) AS data_nascimento,
      CAST(NULL AS STRING) AS naturalidade,
      CAST(NULL AS STRING) AS nacionalidade,
      CAST(NULL AS STRING) AS raca_cor,
      CAST(NULL AS STRING) AS deficiencia,
      CAST(NULL AS STRING) AS bolsa_familia,
      CAST(NULL AS STRING) AS endereco,
      CAST(NULL AS STRING) AS bairro,
      CAST(NULL AS STRING) AS municipio,
      CAST(NULL AS STRING) AS cep,
  FROM union_tables
)

UNION ALL

(
WITH UltimaDataPorCPF AS (
    SELECT
      cpf,
      MAX(data_particao) AS max_data_particao
    FROM `rj-smas.protecao_social_cadunico.documento_pessoa`
    GROUP BY cpf
  ),
  UltimaDataIdentificacao AS (
    SELECT
      id_pessoa,
      MAX(data_particao) AS max_data_particao
    FROM `rj-smas.protecao_social_cadunico.identificacao_primeira_pessoa`
    GROUP BY id_pessoa
  ),
  UltimaIdentificacao AS (
    SELECT
      *
    FROM `rj-smas.protecao_social_cadunico.identificacao_primeira_pessoa` pp
    INNER JOIN UltimaDataIdentificacao ident ON ident.max_data_particao = pp.data_particao
      AND ident.id_pessoa = pp.id_pessoa
  ),
  UltimaDataControle AS (
    SELECT
      id_familia,
      MAX(data_particao) AS max_data_particao
    FROM `rj-smas.protecao_social_cadunico.identificacao_controle`
    GROUP BY 1
  ),
  UltimoControle AS (
    SELECT
      control.id_familia,
      CONCAT(tipo_logradouro,
      logradouro,
      numero_logradouro,
      complemento) AS endereco,
      cep,
      id_municipio AS municipio -- TODO: trocar
    FROM `rj-smas.protecao_social_cadunico.identificacao_controle` control
    INNER JOIN UltimaDataControle ultimo ON ultimo.max_data_particao = control.data_particao
      AND ultimo.id_familia = control.id_familia
  ),
  deficiencia AS (
    SELECT 
    ident.id_familia,
    ident.id_membro_familia,
    CASE
      WHEN def.id_familia IS NULL THEN "Não possui"
      WHEN (
                IFNULL(CAST(deficiencia_surdez_leve AS FLOAT64), 0) +
                IFNULL(CAST(deficiencia_surdez_profunda AS FLOAT64), 0) +
                IFNULL(CAST(deficiencia_baixa_visao AS FLOAT64), 0) +
                IFNULL(CAST(deficiencia_fisica AS FLOAT64), 0) +
                IFNULL(CAST(deficiencia_cegueira AS FLOAT64), 0) +
                IFNULL(CAST(deficiencia_mental AS FLOAT64), 0) +
                IFNULL(CAST(deficiencia_transtorno_mental AS FLOAT64), 0) +
                IFNULL(CAST(deficiencia_sindrome_down AS FLOAT64), 0) 
            ) = 0 THEN "Não especificado"
            WHEN (
                IFNULL(CAST(deficiencia_surdez_leve AS FLOAT64), 0) +
                IFNULL(CAST(deficiencia_surdez_profunda AS FLOAT64), 0) +
                IFNULL(CAST(deficiencia_baixa_visao AS FLOAT64), 0) +
                IFNULL(CAST(deficiencia_fisica AS FLOAT64), 0) +
                IFNULL(CAST(deficiencia_cegueira AS FLOAT64), 0) +
                IFNULL(CAST(deficiencia_mental AS FLOAT64), 0) +
                IFNULL(CAST(deficiencia_transtorno_mental AS FLOAT64), 0) +
                IFNULL(CAST(deficiencia_sindrome_down AS FLOAT64), 0) 
            ) > 0 THEN "Deficiência múltipla"
            WHEN (deficiencia_surdez_profunda IS NOT NULL OR deficiencia_surdez_leve IS NOT NULL) THEN "Deficiência auditiva"
            WHEN (deficiencia_transtorno_mental IS NOT NULL OR deficiencia_mental IS NOT NULL OR deficiencia_sindrome_down IS NOT NULL) THEN "Deficiência intelectual"
            WHEN deficiencia_fisica IS NOT NULL THEN "Deficiência física"
            WHEN (deficiencia_cegueira IS NOT NULL OR deficiencia_baixa_visao IS NOT NULL) THEN "Deficiência visual"
    END AS deficiencia
    FROM UltimaIdentificacao ident
    LEFT JOIN `rj-smas.protecao_social_cadunico.pessoa_deficiencia` def ON ident.id_familia = def.id_familia
      AND ident.id_membro_familia = def.id_membro_familia
  )
  SELECT
    DISTINCT
    SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(doc.cpf), r'\.0$', ''), r'^0+', '') AS STRING) AS cpf,
    SHA512(SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(doc.cpf), r'\.0$', ''), r'^0+', '') AS STRING)) AS id_hash,
    UPPER(ident.nome) nome,
    sexo AS genero,
    CAST(data_nascimento AS DATE) AS data_nascimento,
    UPPER(local_nascimento) AS naturalidade,
    UPPER(pais_nascimento) AS nacionalidade,
    raca_cor AS raca_cor,
    def.deficiencia AS deficiencia,
    CAST(NULL AS STRING) AS bolsa_familia,
    endereco,
    CAST(NULL AS STRING) AS bairro,
    municipio AS municipio,
    cep AS cep,
  FROM `rj-smas.protecao_social_cadunico.documento_pessoa` doc
  INNER JOIN UltimaDataPorCPF cpf ON cpf.max_data_particao = doc.data_particao AND cpf.cpf = doc.cpf
  INNER JOIN UltimaIdentificacao ident ON doc.id_membro_familia = ident.id_membro_familia
     AND doc.id_familia = ident.id_familia
  INNER JOIN UltimoControle control ON control.id_familia = doc.id_familia
  INNER JOIN deficiencia def ON doc.id_membro_familia = def.id_membro_familia
     AND doc.id_familia = def.id_familia
  
)
