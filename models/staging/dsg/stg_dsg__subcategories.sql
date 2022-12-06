{{
    config(
        materialized = 'table',
    )
}}

{{ stg_afi__subcategories('dsg') }}
