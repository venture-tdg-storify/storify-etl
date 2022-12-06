{{
    config(
        materialized = 'table',
        alias = 'info',
    )
}}

select
    uuid_string() as uniqueness_key,
    sysdate() as run_timestamp
