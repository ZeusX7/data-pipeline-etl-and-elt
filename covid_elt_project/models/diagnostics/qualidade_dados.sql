with nulos as (
    select
        count(*) as total_linhas,
        count(*) filter (where _id is null) as _id_nulos,
        count(*) filter (where faixa_etaria is null) as faixa_etaria_nulos,
        count(*) filter (where idade is null) as idade_nulos,
        count(*) filter (where sexo is null) as sexo_nulos,
        count(*) filter (where raca_cor is null) as raca_cor_nulos,
        count(*) filter (where municipio is null) as municipio_nulos,
        count(*) filter (where grupo is null) as grupo_nulos,
        count(*) filter (where categoria is null) as categoria_nulos,
        count(*) filter (where lote is null) as lote_nulos,
        count(*) filter (where vacina_fabricante is null) as vacina_fabricante_nulos,
        count(*) filter (where descricao_dose is null) as descricao_dose_nulos,
        count(*) filter (where cnes is null) as cnes_nulos,
        count(*) filter (where sistema_origem is null) as sistema_origem_nulos,
        count(*) filter (where data_vacinacao is null) as data_vacinacao_nulos
    from {{ source('covid_source', 'stg_casos_covid') }}
),

duplicados as (
    select
        count(*) as ids_duplicados
    from (
        select _id
        from {{ source('covid_source', 'stg_casos_covid') }}
        group by _id
        having count(*) > 1
    ) sub
)

select *
from nulos, duplicados
