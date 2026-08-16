{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ ref('bronze_clientes') }}
),
deduplicado as (
    select distinct * from source
),
limpio as (
    select
        cliente_id,
        trim(nombre) as nombre,
        lower(trim(email)) as email,
        trim(ciudad) as ciudad,
        fecha_registro,
        _batch_id,
        _inserted_at,
        row_number() over (
            partition by lower(trim(email))
            order by _inserted_at desc
        ) as rn_email
    from deduplicado
    where email is not null
      and email like '%@%'
)
select
    cliente_id,
    nombre,
    email,
    ciudad,
    fecha_registro,
    _batch_id,
    _inserted_at
from limpio
where rn_email = 1
  and fecha_registro between date('2020-01-01') and current_date()
