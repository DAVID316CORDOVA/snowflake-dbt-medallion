{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ ref('bronze_pedidos') }}
),
deduplicado as (
    -- Deduplication by business key (pedido_id), not by full-row DISTINCT.
    -- Full-row DISTINCT is fragile: any noisy column (like a randomly
    -- generated attributes_extra JSON, or a timestamp) can make two
    -- logically-duplicate rows look "different", defeating the dedup.
    select *,
        row_number() over (
            partition by pedido_id
            order by _inserted_at desc
        ) as rn_pedido
    from source
    qualify rn_pedido = 1
),
con_atributos as (
    select
        pedido_id,
        cliente_id,
        trim(producto) as producto,
        cantidad,
        precio_unitario,
        fecha_pedido,
        estado,
        _batch_id,
        _inserted_at,
        atributos_extra:color::string           as color,
        atributos_extra:garantia_meses::int      as garantia_meses,
        atributos_extra:canal_venta::string      as canal_venta,
        atributos_extra:descuento_pct::int       as descuento_pct,
        atributos_extra:promocion::string        as promocion,
        atributos_extra                          as atributos_extra_raw
    from deduplicado
)
select
    pedido_id,
    cliente_id,
    producto,
    cantidad,
    precio_unitario,
    fecha_pedido,
    estado,
    _batch_id,
    _inserted_at,
    color,
    garantia_meses,
    canal_venta,
    descuento_pct,
    promocion,
    atributos_extra_raw,
    case when cantidad is null or cantidad <= 0 then true else false end as flag_cantidad_invalida,
    case when precio_unitario is null or precio_unitario <= 0 then true else false end as flag_precio_invalido,
    case when fecha_pedido > current_date() then true else false end as flag_fecha_futura
from con_atributos
where fecha_pedido is not null