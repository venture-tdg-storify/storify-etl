{{
    config(
        materialized = 'table',
    )
}}

{{ base_afi__transactions('dsg') }}
