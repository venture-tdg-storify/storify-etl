{{
    config(
        materialized = 'table',
    )
}}

{{ stg_afi__store_monthly_sales('tdg') }}
