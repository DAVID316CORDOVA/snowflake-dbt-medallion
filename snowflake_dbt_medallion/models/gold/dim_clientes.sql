{{
  config(
    materialized='table'
  )
}}

select
    cliente_id,
    nombre,
    email,
    ciudad,
    fecha_registro,
    datediff(day, fecha_registro, current_date()) as dias_como_cliente
from {{ ref('stg_clientes') }}
