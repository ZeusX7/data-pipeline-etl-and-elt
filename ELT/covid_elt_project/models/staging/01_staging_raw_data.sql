-- models/staging/01_staging_raw_data.sql
-- Primeiro estágio: limpeza básica e padronização inicial dos dados brutos

{{ config(materialized='view') }}

WITH raw_data_cleaned AS (
    SELECT 
        *,
        -- Criar ranking para desduplicação (manter apenas 1 registro por pessoa/data/local)
        ROW_NUMBER() OVER (
            PARTITION BY data_vacinacao, cnes, lote, idade, sexo 
            ORDER BY sistema_origem, raca_cor, municipio
        ) as row_rank
    FROM {{ source('raw', 'covid_vacinacao_raw') }}
),

deduplicated_data AS (
    SELECT 
        -- Criando ID único baseado em hash das colunas principais
        {{ dbt_utils.generate_surrogate_key([
            'data_vacinacao', 
            'cnes', 
            'lote', 
            'idade', 
            'sexo'
        ]) }} as id,
        
        -- Convertendo idade para integer com tratamento de nulos/vazios
        CASE 
            WHEN TRIM(idade) = '' OR idade IS NULL THEN NULL
            ELSE CAST(TRIM(idade) AS INTEGER) 
        END as idade,
        
        -- Padronizando sexo para formato simplificado
        CASE 
            WHEN UPPER(TRIM(sexo)) = 'FEMININO' THEN 'F'
            WHEN UPPER(TRIM(sexo)) = 'MASCULINO' THEN 'M'
            WHEN UPPER(TRIM(sexo)) IN ('F', 'M') THEN UPPER(TRIM(sexo))
            ELSE NULL -- Tratar valores inválidos como NULL
        END as sexo,
        
        TRIM(raca_cor) as raca_cor,
        TRIM(municipio) as municipio,
        TRIM(grupo) as grupo,
        
        TRIM(lote) as lote,
        
        -- Limpando vacina_fabricante removendo o número inicial
        CASE 
            WHEN vacina_fabricante LIKE '%-%' 
            THEN TRIM(SUBSTRING(vacina_fabricante FROM POSITION('-' IN vacina_fabricante) + 1))
            ELSE TRIM(vacina_fabricante)
        END as vacina_fabricante,
        
        -- Padronizando descrição da dose com mais variações
        CASE 
            WHEN TRIM(UPPER(descricao_dose)) LIKE '%1%' OR TRIM(UPPER(descricao_dose)) LIKE '%PRIMEIRA%' OR TRIM(UPPER(descricao_dose)) LIKE '%UM%' THEN '1'
            WHEN TRIM(UPPER(descricao_dose)) LIKE '%2%' OR TRIM(UPPER(descricao_dose)) LIKE '%SEGUNDA%' OR TRIM(UPPER(descricao_dose)) LIKE '%DOIS%' THEN '2'
            WHEN TRIM(UPPER(descricao_dose)) LIKE '%3%' OR TRIM(UPPER(descricao_dose)) LIKE '%TERCEIRA%' OR TRIM(UPPER(descricao_dose)) LIKE '%TRÊS%' OR TRIM(UPPER(descricao_dose)) LIKE '%TRES%' THEN '3'
            WHEN TRIM(UPPER(descricao_dose)) LIKE '%4%' OR TRIM(UPPER(descricao_dose)) LIKE '%QUARTA%' OR TRIM(UPPER(descricao_dose)) LIKE '%QUATRO%' THEN '4'
            WHEN TRIM(UPPER(descricao_dose)) LIKE '%5%' OR TRIM(UPPER(descricao_dose)) LIKE '%QUINTA%' OR TRIM(UPPER(descricao_dose)) LIKE '%CINCO%' THEN '4'
            WHEN TRIM(UPPER(descricao_dose)) LIKE '%ÚNICA%' OR TRIM(UPPER(descricao_dose)) LIKE '%UNICA%' OR TRIM(UPPER(descricao_dose)) LIKE '%DOSE ÚNICA%' THEN '1'
            WHEN TRIM(UPPER(descricao_dose)) LIKE '%REFORÇO%' OR TRIM(UPPER(descricao_dose)) LIKE '%REFORCO%' OR TRIM(UPPER(descricao_dose)) LIKE '%BOOST%' THEN '3'
            WHEN TRIM(UPPER(descricao_dose)) LIKE '%ADICIONAL%' THEN '4'
            ELSE '1' -- Default para dose 1 se não conseguir identificar
        END as dose_aplicada,
        
        -- Extraindo apenas o código CNES usando regex
        TRIM(
            SUBSTRING(
                cnes FROM 'CNES:\s*([0-9A-Z]+)'
            )
        ) as cnes_codigo,
        
        -- Extraindo o nome da unidade de saúde
        TRIM(
            SUBSTRING(
                cnes FROM 'CNES:\s*[0-9A-Z]+\s*-\s*(.+)'
            )
        ) as unidade_saude_nome,
        
        TRIM(sistema_origem) as sistema_origem,
        
        -- Convertendo data para formato DATE
        CAST(data_vacinacao AS DATE) as data_vacinacao

    FROM raw_data_cleaned
    WHERE row_rank = 1  -- Manter apenas o primeiro registro de cada grupo
)

SELECT * FROM deduplicated_data
WHERE 
    -- Filtros de qualidade mais rigorosos
    idade IS NOT NULL 
    AND idade BETWEEN 0 AND 120  -- Idades válidas
    AND sexo IS NOT NULL 
    AND sexo IN ('F', 'M')       -- Apenas sexos válidos
    AND data_vacinacao IS NOT NULL
    AND cnes_codigo IS NOT NULL
    AND lote IS NOT NULL
    AND dose_aplicada IS NOT NULL
ORDER BY data_vacinacao, id