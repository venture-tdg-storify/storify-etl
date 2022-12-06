{{
    config(
        materialized = 'table',
    )
}}

select * from {{ ref('stg_dsg__daily_group_sales') }}
