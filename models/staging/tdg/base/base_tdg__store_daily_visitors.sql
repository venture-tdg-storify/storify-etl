{{
    config(
        materialized = 'table',
    )
}}

{{ base_afi__store_daily_visitors('tdg') }}
