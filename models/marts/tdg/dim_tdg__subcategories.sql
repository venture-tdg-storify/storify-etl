{{
    config(
        materialized = 'table',
    )
}}

{{ dim_afi__subcategories('tdg') }}
