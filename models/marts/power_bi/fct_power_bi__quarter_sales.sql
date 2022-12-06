{{
    config(
        materialized = 'table',
    )
}}

{% set tenant_names = get_tenant_codebase_names() %}

with
{% for tenant_name in tenant_names %}
    {{ tenant_name }}_quarter_sales as (
        with
        year_quarters as (
            select distinct
                year(transaction_date) as year,
                quarters.name as quarter
            from {{ ref('stg_%s__transactions' % tenant_name) }}
            cross join (
                select row_number() over (order by seq4()) as name
                from table(generator(rowcount => 4))
            ) as quarters
        ),

        amount_per_quarter_year as (
            select
                year(transaction_date) as year,
                quarter(transaction_date) as quarter,
                sum(amount) as amount
            from {{ ref('stg_%s__transactions' % tenant_name) }}
            group by
                year(transaction_date), quarter(transaction_date)
        )

        select
            year_quarters.quarter,
            year_quarters.year,
            coalesce(quarter_sales.amount, 0) as amount
        from year_quarters
        left join amount_per_quarter_year as quarter_sales
            on year_quarters.year = quarter_sales.year and year_quarters.quarter = quarter_sales.quarter
    )

    {% if not loop.last %}, {% endif %}
{% endfor %}

{% for tenant_name in tenant_names %}
    select
        tenants.name as tenant_name,
        quarter_sales.quarter,
        quarter_sales.year,
        quarter_sales.amount
    from {{ tenant_name }}_quarter_sales as quarter_sales
    inner join {{ ref('tenants') }} as tenants
        on tenants.codebase_name = '{{ tenant_name }}'
    {% if not loop.last %}union all{% endif %}
{% endfor %}
