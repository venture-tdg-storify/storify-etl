{{
    config(
        materialized = 'table',
    )
}}

{{ stg_afi__daily_group_sales('dsg') }}
