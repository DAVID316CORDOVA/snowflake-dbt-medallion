{{
  config(
    materialized='incremental',
    unique_key='pedido_id',
    incremental_strategy='merge'
  )
}}

-- Deduplicates by business key before the MERGE runs. Bronze intentionally
-- keeps duplicate pedido_id rows (it only casts types, it doesn't dedup) --
-- without this QUALIFY, a MERGE on subsequent runs fails with "Duplicate
-- row detected", since MERGE requires the join key to be unique within
-- the batch being merged.
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

qualify row_number() over (
    partition by pedido_id
    order by _inserted_at desc
) = 1
