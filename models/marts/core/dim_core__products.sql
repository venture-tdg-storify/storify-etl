{{
    config(
        materialized = 'table',
        alias = 'products',
    )
}}

{% set tenant_names = get_tenant_codebase_names() %}

with
{% for tenant_name in tenant_names %}
    {{ tenant_name }}_products as (
        select
            products.id,
            products.id as manufacturer_id,
            tenants.id as tenant_id,
            products.category_id,
            products.subcategory_id,
            products.group_id,
            products.subgroup_id,
            products.name,
            products.status,
            case
                when products.type = 'Item' then 0
                when products.type = 'Component' then 1
                when products.type = 'Kit' then 2
            end as type,
            products.code,
            array_to_string(
                array_construct_compact(
                    case
                        when products.is_exclusive then 'exclusive'
                        else null
                    end,
                    case
                        when products.is_new then 'new'
                        else null
                    end
                ),
                ','
            ) as flags,
            array_to_string(
                array_construct_compact(
                    case
                        when products.purchase_status = 'T' then 'Purchase Status - T-Discontinued'
                        when products.purchase_status = 'D' then 'Purchase Status - D-Drop'
                        else null
                    end
                ),
                ','
            ) as tags,
            '["' || products.image_url || '"]' as image_urls
        from {{ ref('stg_%s__products' % tenant_name) }} as products
        inner join {{ ref('tenants') }} as tenants
        where
            tenants.codebase_name = '{{ tenant_name }}'
            and products.is_available_in_app = true
    )
    {% if not loop.last %}, {% endif %}
{% endfor %}

{% for tenant_name in tenant_names %}
    select *
    from {{ tenant_name }}_products
    {% if not loop.last %}union all{% endif %}
{% endfor %}
