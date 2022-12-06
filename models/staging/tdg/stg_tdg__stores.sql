{{
    config(
        materialized = 'table',
    )
}}

{{ stg_afi__stores('tdg') }}
