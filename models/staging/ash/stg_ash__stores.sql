{{
    config(
        materialized = 'table',
    )
}}

select
    id,
    name,
    type,
    is_active,
    floor_sqft,
    address_country,
    address_state,
    address_city,
    address_postal_code
from {{ ref('base_ash__stores') }}
