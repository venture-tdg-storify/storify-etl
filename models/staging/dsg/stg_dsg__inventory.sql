{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail',
        unique_key = ['product_id', 'store_id', 'inventory_date'],
    )
}}

{{ stg_afi__inventory('dsg') }}
