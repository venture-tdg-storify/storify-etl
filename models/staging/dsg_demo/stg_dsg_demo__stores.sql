{{
    config(
        materialized = 'table',
    )
}}

with
stores as (
    select
        *,
        row_number() over (order by id) as rownum
    from {{ ref('stg_dsg__stores') }}
)

select
    stores.* exclude(id, name, rownum),
    {{ id_to_demo_id('stores.id') }} as id,
    cities.name as name
from stores
inner join {{ ref("seed_demo__cities") }} as cities
    on cities.id = stores.rownum
