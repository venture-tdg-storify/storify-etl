{{
    config(
        materialized = 'table',
    )
}}

{% set currency_variables = ['usd_cad'] %}
{% set currencies = ['CAD'] %}

with
recursive all_dates as (
    select '2019-04-01'::date as date
    union all
    select dateadd('day', 1, date) as date
    from all_dates
    where date < current_date()
),

base_currency_exchange_rates as (
    select
        quote_currency_id as currency,
        value,
        date
    from {{ source('financial__economic_essentials', 'fx_rates_timeseries') }}
    where
        variable in (
            {% for currency_variable in currency_variables %}
                '{{ currency_variable }}'
                {% if not loop.last %},{% endif %}
            {% endfor %}
        )
        and date >= '2019-04-01'
),

{% for currency in currencies %}
    {{ currency }}_exchange_rates as (
        select
            '{{ currency }}' as currency,
            all_dates.date,
            base_currency_exchange_rates.value
        from all_dates
        left join base_currency_exchange_rates
            on
                all_dates.date = base_currency_exchange_rates.date
                and base_currency_exchange_rates.currency = '{{ currency }}'
    ),
{% endfor %}

{% for currency in currencies %}
    {{ currency }}_exchange_rates_with_lag as (
        select
            currency,
            date,
            value,
            lag(value) ignore nulls over (
                partition by currency
                order by date
            ) as previous_value
        from {{ currency }}_exchange_rates
    )
    {% if not loop.last %},{% endif %}
{% endfor %}

{% for currency in currencies %}
    select
        currency,
        date,
        coalesce(value, previous_value) as value
    from {{ currency }}_exchange_rates_with_lag
    {% if not loop.last %}union all{% endif %}
{% endfor %}
