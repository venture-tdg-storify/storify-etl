{% macro stg_afi__action_stores(tenant_name) %}
    select
        action_stores.id,
        action_stores.tenant_id,
        action_stores.action_set_id,
        action_stores.action_set_finalized_at,
        action_stores.store_id,
        action_stores.category_id,
        action_stores.subcategory_id,
        action_stores.group_id,
        action_stores.is_add,
        action_stores.principal_action_store_id,
        action_stores.dependent_action_store_id,
        action_stores.completed_by_run_id,
        action_stores.completion_date,
        action_stores.completion_status_str,
        action_stores.created_at,
        action_stores.past_sales,
        action_stores.predicted_sales,
        action_stores.is_manual
    from {{ ref('base_core__action_stores') }} as action_stores
    inner join {{ ref('tenants') }} as tenants
        on tenants.id = action_stores.tenant_id
    where tenants.codebase_name = '{{ tenant_name }}'
{% endmacro %}
