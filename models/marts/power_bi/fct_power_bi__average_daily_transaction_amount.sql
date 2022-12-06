{{
    config(
        materialized = 'table',
    )
}}

{% set tenant_names = get_tenant_codebase_names() %}

with
{% for tenant_name in tenant_names %}
    {{ tenant_name }}_average_daily_transaction_amount as (
        with
        transaction_total as (
            select
                transaction_date,
                sum(amount) as amount
            from {{ ref('stg_%s__transactions' % tenant_name) }}
            group by
                order_id,
                transaction_date
        )

        select
            transaction_date,
            avg(amount) as avg_amount
        from transaction_total
        group by
            transaction_date
    )
    {% if not loop.last %}, {% endif %}
{% endfor %}

{% for tenant_name in tenant_names %}
    select
        tenants.name as tenant_name,
        transaction_date,
        avg_amount
    from {{ tenant_name }}_average_daily_transaction_amount
    inner join {{ ref('tenants') }} as tenants
        on tenants.codebase_name = '{{ tenant_name }}'
    {% if not loop.last %}union all{% endif %}
{% endfor %}
