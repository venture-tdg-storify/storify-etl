{{
    config(
        materialized = 'table',
    )
}}

select
    trim(location_key) as id,
    trim(store_location) as name,
    case
        when retail_group = 'Stores' then 'store'
        when retail_group = 'Online' then 'online'
    end as type,
    case
        when retail_group = 'Online' then false
        else
            coalesce(
                dateadd(year, 2000, close_date) > current_date,
                true
            )
    end as is_active,
    square_footage as floor_sqft,
    'United States' as address_country,
    state as address_state,
    city as address_city,
    zip_code as address_postal_code
from {{ source('ash', 'raw_locations') }}
