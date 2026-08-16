{{
  config(
    materialized='table'
  )
}}

select
    id                                          as pedido_id,
    cliente_id,
    producto,
    try_cast(cantidad as int)                   as cantidad,
    try_cast(precio_unitario as decimal(10,2))  as precio_unitario,
    try_cast(fecha_pedido as date)              as fecha_pedido,
    estado,
    _batch_id,
    atributos_extra,
    _inserted_at
from {{ source('raw_layer', 'pedidos') }}
