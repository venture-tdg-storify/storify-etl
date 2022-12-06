{% macro stg_afi__groups(tenant_name) %}
    with
    sister_subgroups as (
        select distinct group_id
        from {{ ref('stg_%s__sister_subgroups' % tenant_name) }}
    ),

    groups as (
        select distinct group_id
        from {{ ref('base_%s__products' % tenant_name) }}
    )

    select
        group_id as id,
        group_id as name
    from groups
    union
    select
        group_id as id,
        group_id as name
    from sister_subgroups
{% endmacro %}
