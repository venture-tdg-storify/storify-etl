{{
    config(
        materialized = 'table',
    )
}}

select * from {{ ref('stg_dsg__products') }}
