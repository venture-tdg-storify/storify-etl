{% macro stg_afi__store_monthly_sales(tenant_name) %}
    with
    monthly_store_transactions as (
        with
        base_results as (
            select
                store_id,
                date_part('year', transaction_date) as year,
                date_part('month', transaction_date) as month,
                amount
            from {{ ref('base_%s__transactions' % tenant_name) }}
        )

        select
            store_id,
            year,
            month,
            sum(amount) as total_amount
        from base_results
        group by
            store_id,
            year,
            month
    ),

    monthly_store_visitors as (
        with
        base_results as (
            select
                store_id,
                date_part('year', created_date) as year,
                date_part('month', created_date) as month,
                visitor_count
            from {{ ref('base_%s__store_daily_visitors' % tenant_name) }}
        )

        select
            store_id,
            year,
            month,
            sum(visitor_count) as total_visitor_count
        from base_results
        group by
            store_id,
            year,
            month
    )

    select
        monthly_store_transactions.store_id,
        monthly_store_transactions.year,
        monthly_store_transactions.month,
        monthly_store_transactions.total_amount,
        monthly_store_visitors.total_visitor_count,
        case
            when monthly_store_visitors.total_visitor_count = 0
                then null
            else
                monthly_store_transactions.total_amount
                / monthly_store_visitors.total_visitor_count
        end as amount_per_visitor,
        case
            when stores.floor_sqft = 0 then null
            else amount_per_visitor / stores.floor_sqft
        end as amount_per_visitor_per_sqft
    from monthly_store_transactions
    inner join {{ ref('stg_%s__stores' % tenant_name) }} as stores
        on monthly_store_transactions.store_id = stores.id
    inner join monthly_store_visitors
        on monthly_store_transactions.store_id = monthly_store_visitors.store_id
        and monthly_store_transactions.year = monthly_store_visitors.year
        and monthly_store_transactions.month = monthly_store_visitors.month
{% endmacro %}
