{{
    config(
        materialized = 'table',
    )
}}

{{ base_afi__stores('tdg') }}
