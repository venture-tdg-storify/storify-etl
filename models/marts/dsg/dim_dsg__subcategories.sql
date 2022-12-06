{{
    config(
        materialized = 'table',
    )
}}

{{ dim_afi__subcategories('dsg') }}
