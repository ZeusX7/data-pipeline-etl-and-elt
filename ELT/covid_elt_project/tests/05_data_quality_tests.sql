-- tests/data_quality_tests.sql
-- Testes de qualidade dos dados para validar as transformações

-- Teste 1: Verificar se não há IDs duplicados na tabela fato
SELECT 'duplicated_ids' as test_name, COUNT(*) as failures
FROM (
    SELECT id, COUNT(*) as cnt
    FROM {{ ref('04_fact_pessoas_vacinadas') }}
    GROUP BY id
    HAVING COUNT(*) > 1
) duplicates

UNION ALL

-- Teste 2: Verificar se todos os códigos CNES existem na dimensão
SELECT 'missing_cnes_references' as test_name, COUNT(*) as failures
FROM {{ ref('04_fact_pessoas_vacinadas') }} f
LEFT JOIN {{ ref('02_dim_cnes') }} d ON f.cnes_codigo = d.cnes_codigo
WHERE d.cnes_codigo IS NULL

UNION ALL

-- Teste 3: Verificar se todos os lotes existem na dimensão
SELECT 'missing_lote_references' as test_name, COUNT(*) as failures
FROM {{ ref('04_fact_pessoas_vacinadas') }} f
LEFT JOIN {{ ref('03_dim_vacina_lote') }} d ON f.lote = d.lote
WHERE d.lote IS NULL

UNION ALL

-- Teste 4: Verificar se não há valores nulos em campos obrigatórios
SELECT 'null_essential_fields' as test_name, COUNT(*) as failures
FROM {{ ref('04_fact_pessoas_vacinadas') }}
WHERE id IS NULL 
   OR idade IS NULL 
   OR sexo IS NULL 
   OR data_vacinacao IS NULL
   OR cnes_codigo IS NULL
   OR lote IS NULL

UNION ALL

-- Teste 5: Verificar se sexo está padronizado
SELECT 'invalid_sexo_values' as test_name, COUNT(*) as failures
FROM {{ ref('04_fact_pessoas_vacinadas') }}
WHERE sexo NOT IN ('F', 'M')

UNION ALL

-- Teste 6: Verificar se idades estão em faixa válida
SELECT 'invalid_age_values' as test_name, COUNT(*) as failures
FROM {{ ref('04_fact_pessoas_vacinadas') }}
WHERE idade < 0 OR idade > 120

UNION ALL

-- Teste 7: Verificar se doses estão em valores válidos
SELECT 'invalid_dose_values' as test_name, COUNT(*) as failures
FROM {{ ref('04_fact_pessoas_vacinadas') }}
WHERE dose_aplicada NOT IN ('1', '2', '3', '4')