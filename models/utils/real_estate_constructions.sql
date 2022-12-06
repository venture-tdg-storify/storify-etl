{{
    config(
        materialized = 'view',
    )
}}

select
    regexp_replace(geo_id, '^geoId\/', '') as county_fips,
    variable,
    date_part('year', date) as year,
    date_part('month', date) as month,
    value
from {{ source('us_housing__real_estate_essentials', 'us_real_estate_timeseries') }}
where
    regexp_like(geo_id, '^geoId\/\\d{5}$')
    and variable in (
        'UNITS_1_UNIT_M',
        'UNITS_1_UNIT_REP_M',
        'UNITS_2_UNITS_M',
        'UNITS_2_UNITS_REP_M',
        'UNITS_3_4_UNITS_M',
        'UNITS_3_4_UNITS_REP_M',
        'UNITS_5_UNITS_M',
        'UNITS_5_UNITS_REP_M',
        'VALUE_1_UNIT_M',
        'VALUE_1_UNIT_REP_M',
        'VALUE_2_UNITS_M',
        'VALUE_2_UNITS_REP_M',
        'VALUE_3_4_UNITS_M',
        'VALUE_3_4_UNITS_REP_M',
        'VALUE_5_UNITS_M',
        'VALUE_5_UNITS_REP_M'
    )
