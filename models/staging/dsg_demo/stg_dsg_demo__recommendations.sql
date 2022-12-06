{{
    config(
        materialized = 'table',
    )
}}

select
    * exclude(past_sales, predicted_sales, avg_daily_on_floor_sales, store_id),
    {{ obfuscate_sales('past_sales') }} as past_sales,
    {{ obfuscate_sales('predicted_sales') }} as predicted_sales,
    {{ obfuscate_sales('avg_daily_on_floor_sales') }} as avg_daily_on_floor_sales,
    {{ id_to_demo_id('store_id') }} as store_id
from {{ ref('stg_dsg__recommendations') }}
