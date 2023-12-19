SELECT
  LPAD(id_pais, 2, '0') as id_pais,
  CASE 
    WHEN continente = 'América do Sul' THEN '01'
    WHEN continente = 'América Central' THEN '02'
    WHEN continente = 'América do Norte' THEN '03'
    ELSE '00'
  END as id_continente,
  pais,
  capital,
  CASE 
    WHEN STARTS_WITH(capital, "{{var('PAISES_AMERICANOS_LETRA_INICIAL_CAPITAL')}}") THEN CONCAT("Começa com ", "{{var('PAISES_AMERICANOS_LETRA_INICIAL_CAPITAL')}}")
    ELSE CONCAT("Não começa com ", "{{var('PAISES_AMERICANOS_LETRA_INICIAL_CAPITAL')}}") 
    END indicador_capital_com_{{var('PAISES_AMERICANOS_LETRA_INICIAL_CAPITAL')}}
FROM `rj-escritorio-dev.test_formacao_staging.test_table`