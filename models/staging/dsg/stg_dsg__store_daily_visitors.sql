{{
    config(
        materialized = 'table',
    )
}}

{{ stg_afi__store_daily_visitors('dsg') }}
