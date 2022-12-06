{{
    config(
        materialized = 'table',
    )
}}

{{ stg_afi__action_stores('tdg') }}
