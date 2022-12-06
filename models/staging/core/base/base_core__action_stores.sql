{{
    config(
        materialized = 'table',
    )
}}

select
    id,
    tenant_id,
    action_set_id,
    action_set_finalized_at,
    store_external_id as store_id,
    category_external_id as category_id,
    subcategory_external_id as subcategory_id,
    group_external_id as group_id,
    is_add,
    principal_action_store_id,
    dependent_action_store_id,
    completed_by_run_id,
    completion_date,
    completion_status_str,
    created_at,
    past_sales,
    predicted_sales,
    is_manual
from {{ source('core', 'action_stores') }}
