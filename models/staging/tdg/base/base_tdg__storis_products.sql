{{
    config(
        materialized = 'table',
    )
}}

{{ base_afi__storis_products('tdg') }}
