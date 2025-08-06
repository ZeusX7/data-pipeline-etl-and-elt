-- models/marts/03_dim_vacina_lote.sql
-- Dimensão de lotes de vacinas com informações do fabricante

{{ config(materialized='table') }}

WITH vacina_lote_ranked AS (
    SELECT
        lote,
        vacina_fabricante,
        ROW_NUMBER() OVER (PARTITION BY lote ORDER BY vacina_fabricante) as rn
    FROM {{ ref('01_staging_raw_data') }}
    WHERE lote IS NOT NULL
      AND TRIM(lote) != ''
      AND vacina_fabricante IS NOT NULL
),

vacina_lote_unique AS (
    SELECT
        lote,
        vacina_fabricante
    FROM vacina_lote_ranked
    WHERE rn = 1
),

vacina_parsed AS (
    SELECT 
        lote,
        vacina_fabricante,
        
        -- Extraindo nome da vacina (parte antes dos parênteses)
        TRIM(
            CASE 
                WHEN POSITION('(' IN vacina_fabricante) > 0 
                THEN SUBSTRING(vacina_fabricante FROM 1 FOR POSITION('(' IN vacina_fabricante) - 1)
                ELSE vacina_fabricante
            END
        ) as nome,
        
        -- Extraindo fabricante (parte entre parênteses)
        TRIM(
            CASE 
                WHEN POSITION('(' IN vacina_fabricante) > 0 
                THEN REPLACE(
                    REPLACE(
                        SUBSTRING(vacina_fabricante FROM POSITION('(' IN vacina_fabricante) + 1), 
                        ')', ''
                    ), 
                    '(', ''
                )
                ELSE NULL
            END
        ) as fabricante,
        
        -- Identificando se é pediátrica
        CASE 
            WHEN UPPER(vacina_fabricante) LIKE '%PEDIÁTRICA%' 
              OR UPPER(vacina_fabricante) LIKE '%PEDIATRICA%'
            THEN TRUE
            ELSE FALSE
        END as pediatrica
        
    FROM vacina_lote_unique
)

SELECT 
    lote,
    nome,
    fabricante,
    pediatrica,
    CURRENT_TIMESTAMP as created_at
FROM vacina_parsed

UNION ALL

-- Adiciona o lote DESCONHECIDO para registros com lote nulo ou vazio na fato
SELECT
    'DESCONHECIDO' as lote,
    'Desconhecido' as nome,
    'Desconhecido' as fabricante,
    FALSE as pediatrica,
    CURRENT_TIMESTAMP as created_at

ORDER BY lote
