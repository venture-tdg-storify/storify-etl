{{
    config(
        materialized = 'table',
    )
}}

select
    trim(location_key) as store_id,
    transaction_date as created_date,
    derived_ups::number(38,2) as visitor_count
from {{ source('ash', 'raw_traffic') }}
