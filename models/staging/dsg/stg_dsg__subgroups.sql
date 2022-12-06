{{
    config(
        materialized = 'table',
    )
}}

{{ stg_afi__subgroups('dsg') }}
