{{
    config(
        materialized = 'table',
    )
}}

{{ dim_afi__stores('dsg') }}
