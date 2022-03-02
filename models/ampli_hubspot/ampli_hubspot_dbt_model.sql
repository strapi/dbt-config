{% set json_column_query %}
select distinct
  key as column_name

from {{ source('airbyte_amplitude', '_airbyte_raw_amplitude_events') }},

lateral flatten(input=>_airbyte_data) json
{% endset %}

{% set results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

with data_table as (
  select

  _airbyte_data:device_id as device_id,
  _airbyte_data:user_id as user_id,
  _airbyte_data:uuid as uuid,
  _airbyte_data:start_version as start_version,
  _airbyte_data:user_properties.gaClientID as ga_client_id,
  _airbyte_data:user_properties.numberOfRoles as number_of_roles,
  _airbyte_data:user_properties.numberOfUsers as number_of_users,
  _airbyte_data:event_properties.referer as referer

  from {{ source('airbyte_amplitude', '_airbyte_raw_amplitude_events') }}
),

final as (
  select * from data_table
)

select distinct * from final