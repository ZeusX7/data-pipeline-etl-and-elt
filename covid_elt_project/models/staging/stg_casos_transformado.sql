with base as (
    select
        *,
        row_number() over (partition by _id order by data_vacinacao) as rn
    from {{ source('covid_source', 'stg_casos_covid') }}
)

select
    _id,
    faixa_etaria,
    idade,
    sexo,
    raca_cor,
    municipio,
    grupo,
    categoria,
    lote,
    vacina_fabricante,
    descricao_dose,
    cnes,
    sistema_origem,
    data_vacinacao
from base
where rn = 1
