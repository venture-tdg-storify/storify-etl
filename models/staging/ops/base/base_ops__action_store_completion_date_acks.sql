{{
    config(
        materialized = 'table',
    )
}}

{% set table_source = source('ops', 'action_store_completion_date_acks') %}
{% set table_relation = adapter.get_relation(database = table_source.database, schema = table_source.schema, identifier = table_source.name) %}

{% if table_relation is not none %}
    select action_store_id from {{ table_source }}
{% else %}
    select null::integer as action_store_id where false
{% endif %}
