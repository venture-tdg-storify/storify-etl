{{
    config(
        materialized = 'table',
    )
}}

{{ stg_afi__transactions('dsg') }}
