{{ config(
    materialized="table"
) }}

SELECT 
    DISTINCT 
    cpf,
    SHA512(SAFE_CAST(REGEXP_REPLACE(TRIM(cpf), r'\.0$', '') AS STRING)) AS id_hash,
    numfunc AS id_funcionario -- TODO: confirmar se numfunc é o id_funcionario
FROM `rj-smfp.recursos_humanos_ergon.fita_banco`

-- on-run-end: https://github.com/dbt-labs/dbt-core/issues/6234#issuecomment-1320766483:~:text=%22Temp%22%20tables%20within%20a%20dbt%20%22session%22
-- quero que essa tabela seja temporária, por isso vou excluí-la no on-run-end
-- post inicial https://discourse.getdbt.com/t/is-there-any-way-to-create-temporary-tables-ephemeral-not-suitable-in-dbt/8411