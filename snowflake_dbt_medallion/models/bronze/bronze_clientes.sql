{{
  config(
    materialized='table'
  )
}}

select
    id                                  as cliente_id,
    nombre,
    email,
    ciudad,
    try_cast(fecha_registro as date)    as fecha_registro,
    _batch_id,
    _inserted_at
from {{ source('raw_layer', 'clientes') }}
