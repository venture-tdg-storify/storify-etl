{{
    config(
        materialized = 'table',
    )
}}

{{ dim_afi__products('dsg') }}
