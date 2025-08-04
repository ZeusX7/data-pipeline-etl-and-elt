WITH total_linhas AS (
    SELECT COUNT(*) AS total FROM {{ source('covid_source', 'stg_casos_covid') }}
),

contagem_vazios AS (
    SELECT
        SUM(CASE WHEN faixa_etaria = '' THEN 1 ELSE 0 END) AS faixa_etaria_vazios,
        SUM(CASE WHEN idade = '' THEN 1 ELSE 0 END) AS idade_vazios,
        SUM(CASE WHEN sexo = '' THEN 1 ELSE 0 END) AS sexo_vazios,
        SUM(CASE WHEN raca_cor = '' THEN 1 ELSE 0 END) AS raca_cor_vazios,
        SUM(CASE WHEN municipio = '' THEN 1 ELSE 0 END) AS municipio_vazios,
        SUM(CASE WHEN grupo = '' THEN 1 ELSE 0 END) AS grupo_vazios,
        SUM(CASE WHEN categoria = '' THEN 1 ELSE 0 END) AS categoria_vazios,
        SUM(CASE WHEN lote = '' THEN 1 ELSE 0 END) AS lote_vazios,
        SUM(CASE WHEN vacina_fabricante = '' THEN 1 ELSE 0 END) AS vacina_fabricante_vazios,
        SUM(CASE WHEN descricao_dose = '' THEN 1 ELSE 0 END) AS descricao_dose_vazios,
        SUM(CASE WHEN cnes = '' THEN 1 ELSE 0 END) AS cnes_vazios,
        SUM(CASE WHEN sistema_origem = '' THEN 1 ELSE 0 END) AS sistema_origem_vazios
    FROM {{ source('covid_source', 'stg_casos_covid') }}
),

percentuais AS (
    SELECT
        total,
        faixa_etaria_vazios,
        (faixa_etaria_vazios::float / total) * 100 AS faixa_etaria_percentual,
        idade_vazios,
        (idade_vazios::float / total) * 100 AS idade_percentual,
        sexo_vazios,
        (sexo_vazios::float / total) * 100 AS sexo_percentual,
        raca_cor_vazios,
        (raca_cor_vazios::float / total) * 100 AS raca_cor_percentual,
        municipio_vazios,
        (municipio_vazios::float / total) * 100 AS municipio_percentual,
        grupo_vazios,
        (grupo_vazios::float / total) * 100 AS grupo_percentual,
        categoria_vazios,
        (categoria_vazios::float / total) * 100 AS categoria_percentual,
        lote_vazios,
        (lote_vazios::float / total) * 100 AS lote_percentual,
        vacina_fabricante_vazios,
        (vacina_fabricante_vazios::float / total) * 100 AS vacina_fabricante_percentual,
        descricao_dose_vazios,
        (descricao_dose_vazios::float / total) * 100 AS descricao_dose_percentual,
        cnes_vazios,
        (cnes_vazios::float / total) * 100 AS cnes_percentual,
        sistema_origem_vazios,
        (sistema_origem_vazios::float / total) * 100 AS sistema_origem_percentual
    FROM contagem_vazios
    CROSS JOIN total_linhas
)

SELECT 'faixa_etaria' AS coluna, faixa_etaria_vazios AS vazios, faixa_etaria_percentual AS percentual FROM percentuais WHERE faixa_etaria_percentual > 50
UNION ALL
SELECT 'idade', idade_vazios, idade_percentual FROM percentuais WHERE idade_percentual > 50
UNION ALL
SELECT 'sexo', sexo_vazios, sexo_percentual FROM percentuais WHERE sexo_percentual > 50
UNION ALL
SELECT 'raca_cor', raca_cor_vazios, raca_cor_percentual FROM percentuais WHERE raca_cor_percentual > 50
UNION ALL
SELECT 'municipio', municipio_vazios, municipio_percentual FROM percentuais WHERE municipio_percentual > 50
UNION ALL
SELECT 'grupo', grupo_vazios, grupo_percentual FROM percentuais WHERE grupo_percentual > 50
UNION ALL
SELECT 'categoria', categoria_vazios, categoria_percentual FROM percentuais WHERE categoria_percentual > 50
UNION ALL
SELECT 'lote', lote_vazios, lote_percentual FROM percentuais WHERE lote_percentual > 50
UNION ALL
SELECT 'vacina_fabricante', vacina_fabricante_vazios, vacina_fabricante_percentual FROM percentuais WHERE vacina_fabricante_percentual > 50
UNION ALL
SELECT 'descricao_dose', descricao_dose_vazios, descricao_dose_percentual FROM percentuais WHERE descricao_dose_percentual > 50
UNION ALL
SELECT 'cnes', cnes_vazios, cnes_percentual FROM percentuais WHERE cnes_percentual > 50
UNION ALL
SELECT 'sistema_origem', sistema_origem_vazios, sistema_origem_percentual FROM percentuais WHERE sistema_origem_percentual > 50
