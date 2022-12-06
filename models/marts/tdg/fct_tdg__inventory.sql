{{
    config(
        materialized = 'view',
    )
}}

{{ fct_afi__inventory('tdg') }}
