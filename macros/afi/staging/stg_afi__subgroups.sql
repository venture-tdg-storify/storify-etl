{% macro stg_afi__subgroups(tenant_name) %}
    with
    subgroups as (
        select distinct subgroup_id
        from {{ ref('base_%s__products' % tenant_name) }}
    )

    select
        subgroup_id as id,
        subgroup_id as name
    from subgroups
{% endmacro %}
