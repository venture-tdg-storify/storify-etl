{{
    config(
        materialized = 'table',
    )
}}

{{ dim_afi__products('tdg') }}
