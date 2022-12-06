{{
    config(
        materialized = 'table',
    )
}}

select * from {{ ref('base_dsg__predictions_info') }}
