{{
    config(
        materialized = 'table',
    )
}}

{{ stg_afi__action_store_sales('dsg') }}
