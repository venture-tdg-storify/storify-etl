{{
    config(
        materialized = 'table',
    )
}}

{{ stg_afi__groups('dsg') }}
