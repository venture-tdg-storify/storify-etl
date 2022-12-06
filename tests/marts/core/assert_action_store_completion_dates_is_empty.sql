{{
    config(
        severity = 'warn',
    )
}}

select * from {{ ref('fct_core__action_store_completion_dates') }}
