{{
    config(
        materialized = 'table',
    )
}}

{{ stg_afi__transactions('tdg') }}
