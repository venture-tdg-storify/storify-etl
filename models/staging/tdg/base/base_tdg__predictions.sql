{{
    config(
        materialized = 'table',
    )
}}

{{ base_afi__predictions('tdg') }}
