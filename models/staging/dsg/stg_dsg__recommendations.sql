{{
    config(
        materialized = 'table',
    )
}}

{{ stg_afi__recommendations('dsg') }}
