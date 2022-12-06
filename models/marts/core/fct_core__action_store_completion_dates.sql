{{
    config(
        materialized = 'table',
        alias = 'action_store_completion_dates',
    )
}}

{% set tenant_names = get_tenant_codebase_names('ash') %}

with
{% for tenant_name in tenant_names %}
    {{ tenant_name }}_action_stores as (
        select
            action_store_id,
            completion_date
        from {{ ref('stg_%s__action_store_completion_dates' % tenant_name) }}
    )
    {% if not loop.last %}, {% endif %}
{% endfor %}

{% for tenant_name in tenant_names %}
    select * from {{ tenant_name }}_action_stores
    {% if not loop.last %}union all{% endif %}
{% endfor %}
