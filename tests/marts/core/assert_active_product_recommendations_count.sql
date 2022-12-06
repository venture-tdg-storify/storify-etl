select
    count(*)
from {{ ref('fct_core__recommendations') }} as recommendations
inner join {{ ref('dim_core__products') }} as products
    on products.id = recommendations.product_id
    and products.tenant_id = recommendations.tenant_id
where
    products.status = 'active'
group by
    recommendations.tenant_id,
    recommendations.product_id
having count(*) <> (
    select count(*)
    from {{ ref('dim_core__stores') }}
    where
        tenant_id = recommendations.tenant_id
        and is_active
)

