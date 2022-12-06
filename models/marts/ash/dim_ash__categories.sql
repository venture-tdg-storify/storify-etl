{{
    config(
        materialized = 'table',
    )
}}

select
    id,
    name
from {{ ref('stg_ash__categories') }}
