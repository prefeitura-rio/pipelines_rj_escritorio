-- Dados escolares

(
  WITH UltimaDataPorCPF AS (
    SELECT
      cpf,
      MAX(data_particao) AS max_data_particao
    FROM `rj-sme.educacao_basica_staging.aluno_historico`
    GROUP BY cpf
  )

  SELECT
    a.cpf,
    SAFE_CAST(REGEXP_REPLACE(TRIM(a.cpf), r'\.0$', '') AS STRING) AS id_hash,
    a.nome,
    a.sexo genero, -- "Feminino"/"Masculino"
    a.datanascimento AS data_nascimento,
    UPPER(a.naturalidade) naturalidade,
    UPPER(a.nacionalidade) nacionalidade,
    UPPER(a.raca_cor) raca_cor, -- Parda/Preta/Branca/Amarela/Indígena/Sem Informação/Não declarada/
    UPPER(REGEXP_REPLACE(TRIM(a.deficiencia), r'\*', '')) deficiencia, -- Sem Deficiência	 Deficiência intelectual	 Deficiência física	  Transtorno do espectro autista	*Deficiência múltipla	  Altas habilidades/superdotação	 Deficiência auditiva	 Visão monocular	 Baixa visão	 Surdez	 Cegueira	 Surdocegueira	TGD/Transtornos Invasivos sem outra especificação
    a.bolsa_familia,
    UPPER(a.endereco) endereco,
    UPPER(a.bairro) bairro,
    NULL AS municipio,
    a.cep
  FROM `rj-sme.educacao_basica_staging.aluno` AS a
  INNER JOIN UltimaDataPorCPF AS b
  ON a.cpf = b.cpf AND a.data_particao = b.max_data_particao
)

UNION ALL

(
  WITH UltimaDataPorCPF AS (
    SELECT
        DISTINCT
        SAFE_CAST(REGEXP_REPLACE(TRIM(da.cpf_cnpj), r'\.0$', '') AS STRING) AS cpf,
        -- SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(da.cpf_cnpj), r'\.0$', '') AS STRING)) AS id_hash,
        nome,
        NULL AS genero,
        NULL AS data_nascimento,
        NULL AS naturalidade,
        NULL AS nacionalidade,
        NULL AS raca_cor,
        NULL AS deficiencia,
        NULL AS bolsa_familia,
        NULL AS endereco,
        NULL AS bairro,
        NULL AS municipio,
        NULL AS cep,
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

(
  SELECT
    DISTINCT
    SAFE_CAST(REGEXP_REPLACE(TRIM(id_cpf), r'\.0$', '') AS STRING) as cpf,
    SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(id_cpf), r'\.0$', '') AS STRING)) AS id_hash,
    UPPER(nome) AS nome,
    UPPER(CASE WHEN sexo="F" THEN "Feminino" WHEN sexo="M" THEN "Masculino" ELSE "Outro" END) AS genero,
    data_nascimento,
    UPPER(municipio_nascimento) AS naturalidade,
    UPPER(nacionalidade) nacionalidade,
    NULL AS raca_cor, -- TODO: outra tabela
    CASE
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
    NULL AS bolsa_familia,
    UPPER(CONCAT(tipo_logradouro, logradouro, numero_porta, complemento_numero_porta)) AS endereco,
    UPPER(bairro) AS bairro,
    UPPER(municipio) AS municipio,
    cep
  FROM
    `rj-smfp.recursos_humanos_ergon.funcionario`
)


UNION ALL


UNION ALL




--  select distinct 
--  deficiente,
--  CASE
--     WHEN deficiente IS NOT NULL THEN
--       CASE
--         WHEN CONCAT(
--           IFNULL(deficiente_auditivo, ''),
--           IFNULL(deficiente_fisico, ''),
--           IFNULL(deficiente_visual, ''),
--           IFNULL(deficiente_mental, ''),
--           IFNULL(deficiente_intelectual, '')
--         ) = '' THEN "Não especificado"
--         WHEN UPPER(CONCAT(
--           IFNULL(deficiente_auditivo, ''),
--           IFNULL(deficiente_fisico, ''),
--           IFNULL(deficiente_visual, ''),
--           IFNULL(deficiente_mental, ''),
--           IFNULL(deficiente_intelectual, '')
--         )) != "S" THEN "Deficiência múltipla"
--         WHEN deficiente_auditivo IS NOT NULL THEN "Deficiência auditiva"
--         WHEN (deficiente_intelectual IS NOT NULL) OR (deficiente_mental IS NOT NULL) THEN "Deficiência intelectual"
--         WHEN deficiente_fisico IS NOT NULL THEN "Deficiência física"
--         WHEN deficiente_visual IS NOT NULL THEN "Deficiência visual"
--       END
--     ELSE NULL
--   END AS deficiencia,
--  tipo_deficiencia,
--  CONCAT(
--     IFNULL(deficiente_auditivo, ''),
--     IFNULL(deficiente_fisico, ''),
--     IFNULL(deficiente_visual, ''),
--     IFNULL(deficiente_mental, ''),
--     IFNULL(deficiente_intelectual, '')
--   )
--  ,deficiente_auditivo
--  ,deficiente_fisico
--  ,deficiente_visual
--  ,deficiente_mental
--  ,deficiente_intelectual 
 
--  FROM 
--     `rj-smfp.recursos_humanos_ergon.funcionario`
--     where deficiente is not null and deficiente != "N"
-- order by 1,3
