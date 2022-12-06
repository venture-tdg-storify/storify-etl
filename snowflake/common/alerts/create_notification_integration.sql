create or replace notification integration azure_event_grid_integration_{{ target.name }}
  enabled = true
  direction = outbound
  type = queue
  notification_provider = azure_event_grid
  azure_event_grid_topic_endpoint = 'find_me_at_azure_portal'
  azure_tenant_id = 'find_me_at_azure_portal';

grant usage on integration azure_event_grid_integration_dev to role dbt_cloud_{{ target.name }}_role;
