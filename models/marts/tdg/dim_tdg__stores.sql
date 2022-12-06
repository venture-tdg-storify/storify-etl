{{
    config(
        materialized = 'table',
    )
}}

{{ dim_afi__stores('tdg') }}
