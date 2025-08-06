-- models/marts/04_fact_pessoas_vacinadas.sql
-- Tabela fato principal com dados de pessoas vacinadas (versão simplificada)

{{ config(materialized='table') }}

SELECT 
    id,
    idade,
    sexo,
    raca_cor,
    municipio,
    grupo,
    CASE 
        WHEN lote IS NULL OR TRIM(lote) = '' THEN 'DESCONHECIDO'
        ELSE lote
    END AS lote,
    dose_aplicada,
    cnes_codigo,
    sistema_origem,
    data_vacinacao,
    
    -- Adicionando campos calculados
    EXTRACT(YEAR FROM data_vacinacao) as ano_vacinacao,
    EXTRACT(MONTH FROM data_vacinacao) as mes_vacinacao,
    EXTRACT(DOW FROM data_vacinacao) as dia_semana,
    
    -- Classificação etária
    CASE 
        WHEN idade BETWEEN 0 AND 17 THEN 'Menor de 18 anos'
        WHEN idade BETWEEN 18 AND 29 THEN '18 a 29 anos'
        WHEN idade BETWEEN 30 AND 39 THEN '30 a 39 anos'
        WHEN idade BETWEEN 40 AND 49 THEN '40 a 49 anos'
        WHEN idade BETWEEN 50 AND 59 THEN '50 a 59 anos'
        WHEN idade BETWEEN 60 AND 69 THEN '60 a 69 anos'
        WHEN idade >= 70 THEN '70+ anos'
        ELSE 'Não informado'
    END as faixa_etaria_calculada,
    
    CURRENT_TIMESTAMP as created_at

FROM {{ ref('01_staging_raw_data') }}
ORDER BY data_vacinacao, id
