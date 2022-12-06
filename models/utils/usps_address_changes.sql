{{
    config(
        materialized = 'view',
    )
}}

select
    regexp_replace(geo_id, '^zip/') as zip_code,
    variable,
    date_part('year', date) as year,
    date_part('month', date) as month,
    value
from {{ source('us_housing__real_estate_essentials', 'usps_address_change_timeseries') }}
where
    geo_id like 'zip/%'
    and variable in (
        'inbound_address_changes_businesses',
        'inbound_address_changes_families',
        'inbound_address_changes_individuals',
        'inbound_address_changes_permanent',
        'inbound_address_changes_temporary',
        'outbound_address_changes_businesses',
        'outbound_address_changes_families',
        'outbound_address_changes_individuals',
        'outbound_address_changes_permanent',
        'outbound_address_changes_temporary'
    )
