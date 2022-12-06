{{
    config(
        materialized = 'table',
    )
}}

{{ dim_afi__categories('dsg') }}
