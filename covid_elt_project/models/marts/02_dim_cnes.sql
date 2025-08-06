-- models/marts/02_dim_cnes.sql
-- Dimensão de estabelecimentos de saúde (CNES)

{{ config(materialized='table') }}

WITH cnes_ranked AS (
    SELECT
        cnes_codigo,
        unidade_saude_nome,
        ROW_NUMBER() OVER (
            PARTITION BY cnes_codigo
            ORDER BY unidade_saude_nome
        ) as rn
    FROM {{ ref('01_staging_raw_data') }}
    WHERE cnes_codigo IS NOT NULL
      AND unidade_saude_nome IS NOT NULL
),

cnes_unique AS (
    SELECT
        cnes_codigo,
        unidade_saude_nome
    FROM cnes_ranked
    WHERE rn = 1
)

SELECT 
    cnes_codigo,
    unidade_saude_nome,
    CURRENT_TIMESTAMP as created_at
FROM cnes_unique
ORDER BY cnes_codigo
