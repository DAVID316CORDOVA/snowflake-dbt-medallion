{{
  config(
    materialized='view'
  )
}}

select
    p.pedido_id,
    p.cliente_id,
    f.key   as atributo_nombre,
    f.value as atributo_valor
from {{ ref('bronze_pedidos') }} p,
     lateral flatten(input => p.atributos_extra) f
