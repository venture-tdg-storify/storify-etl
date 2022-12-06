{{
    config(
        materialized = 'table',
    )
}}

{{ stg_afi__categories('dsg') }}
