{% macro base_afi__transactions(tenant_name) %}
    with
    transactions as (
        select
            order_id,
            regexp_replace(product_id, '^AFHS-') as product_id,
            location_id as store_id,
            sell_price::number(38, 2) as amount,
            qty::integer as quantity,
            written_date as transaction_date
        from {{ source(tenant_name, 'raw_transactions') }}
        union all
        select
            order_id,
            regexp_replace(product_id, '^AFHS-') as product_id,
            location_id as store_id,
            sell_price::number(38, 2) as amount,
            qty::integer as quantity,
            written_date as transaction_date
        from {{ source(tenant_name, 'raw_old_transactions') }}
    ),

    ranked_transactions as (
        select
            *,
            rank() over (
                partition by order_id
                order by transaction_date desc
            ) as rank
        from transactions
    )

    select * exclude(rank)
    from ranked_transactions
    where rank = 1
{% endmacro %}
