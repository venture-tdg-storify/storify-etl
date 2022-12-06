{{
    config(
        materialized = 'table',
    )
}}

{{ stg_afi__subcategories('tdg') }}
