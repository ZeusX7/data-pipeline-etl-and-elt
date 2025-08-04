with base as (
    select *,
           row_number() over (partition by _id order by data_aplicacao) as row_num
    from {{ source('public', 'stg_casos_covid') }}
),

sem_duplicatas as (
    select *
    from base
    where row_num = 1
)

select
    _id,
    coalesce(faixa_etaria, 'Não informada') as faixa_etaria,
    sexo,
    coalesce(cor_raca, 'Não informada') as cor_raca,
    municipio,
    coalesce(profissao, 'Desconhecida') as profissao,
    vacina,
    data_aplicacao
from sem_duplicatas
