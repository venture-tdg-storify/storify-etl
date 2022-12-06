{{
    config(
        materialized = 'table',
        alias = 'recommendations',
    )
}}

{% set tenant_names = get_tenant_codebase_names() %}

with
{% for tenant_name in tenant_names %}
    {{ tenant_name }}_recommendations as (
        select
            tenants.id as tenant_id,
            recommendations.product_id,
            recommendations.store_id,
            recommendations.is_on_floor,
            recommendations.floor_date,
            recommendations.past_sales,
            recommendations.avg_daily_on_floor_sales,
            recommendations.predicted_sales,
            array_to_string(recommendations.flags, ',') as flags  
        from
            {{ ref('stg_%s__recommendations' % tenant_name) }}
                as recommendations
        inner join {{ ref('tenants') }} as tenants
        where
            tenants.codebase_name = '{{ tenant_name }}'
            and recommendations.is_available_in_app = true
    )
    {% if not loop.last %}, {% endif %}
{% endfor %}

{% for tenant_name in tenant_names %}
    select *
    from {{ tenant_name }}_recommendations
    {% if not loop.last %}union all{% endif %}
{% endfor %}
