{{
    config(
        materialized = 'table',
    )
}}

{{ fct_afi__store_monthly_sales('tdg') }}
