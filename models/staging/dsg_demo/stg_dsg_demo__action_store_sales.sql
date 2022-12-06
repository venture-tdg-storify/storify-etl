{{
    config(
        materialized = 'table',
    )
}}

select * from {{ ref('stg_dsg__action_store_sales') }}
