{{
    config(
        materialized = 'view',
    )
}}

{{ fct_afi__store_daily_visitors('tdg') }}
