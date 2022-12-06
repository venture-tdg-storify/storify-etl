{{
    config(
        materialized = 'table',
    )
}}

{{ stg_afi__kits('tdg') }}
