{% snapshot clientes_snapshot %}

{{
  config(
    target_schema='silver',
    unique_key='cliente_id',
    strategy='timestamp',
    updated_at='_inserted_at'
  )
}}

select
    cliente_id,
    nombre,
    email,
    ciudad,
    fecha_registro,
    _inserted_at
from {{ ref('stg_clientes') }}

{% endsnapshot %}
