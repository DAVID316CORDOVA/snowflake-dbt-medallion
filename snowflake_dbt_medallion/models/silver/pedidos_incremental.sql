{{
  config(
    materialized='incremental',
    unique_key='pedido_id',
    incremental_strategy='merge'
  )
}}

-- Incremental model: on the first run (or with --full-refresh), this
-- processes the entire bronze_pedidos table. On subsequent runs, the
-- is_incremental() block filters to only rows with a newer _inserted_at
-- than what's already in this table -- avoiding a full reprocess every
-- time, which matters once the source table grows large.
select
    pedido_id,
    cliente_id,
    trim(producto) as producto,
    cantidad,
    precio_unitario,
    fecha_pedido,
    estado,
    _batch_id,
    _inserted_at
from {{ ref('bronze_pedidos') }}

{% if is_incremental() %}
where _inserted_at > (select coalesce(max(_inserted_at), '1900-01-01') from {{ this }})
{% endif %}
