{{
    config(
        materialized = 'view',
    )
}}

{{ fct_afi__transactions('tdg') }}
