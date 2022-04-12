{% set json_column_query %}
select distinct key as column_name

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
    
  _airbyte_data:event_properties.licenseSubscriptionId as license_subscription_id,  
  _airbyte_data:start_version as start_version,
  _airbyte_data:version_name as current_version,
  _airbyte_data:user_properties.numberOfRoles as number_of_roles,
  _airbyte_data:user_properties.numberOfUsers as number_of_users,
  _airbyte_data:user_properties.numberOfI18nLocales as i_18_locales,
  _airbyte_data:user_properties.numberOfI18nContentTypes as i_18_content_types, 
  _airbyte_data:user_properties.plugins as plugins,
  _airbyte_data:user_properties.providers as providers,
  _airbyte_data:user_properties.licenseType as license_type,
  _airbyte_data:user_properties.licenseIsTrial as license_is_trial,
  _airbyte_data:event_properties.projectType as project_type,
  _airbyte_data:client_event_time as event_time
  

  from AIRBYTE_DB.AIRBYTE_SCHEMA._airbyte_raw_amplitude_events
),

license_data_not_null as (
    select * from data_table where license_subscription_id is not null order by license_subscription_id
),

distinct_license_column as (
    select distinct license_subscription_id as license_id from license_data_not_null
),

distinct_data as (
    select d.license_id, l.*
    from distinct_license_column as d
    join (
        select *,
        row_number() over (
            partition by li.license_subscription_id
            order by li.event_time
        ) as row_num
        from license_data_not_null as li
    ) as l
    on l.license_subscription_id = d.license_id and row_num = 1
),

filtered_data as (
    select
    license_subscription_id,
    start_version,
    current_version,
    cast(number_of_roles as int) as number_of_roles,
    cast(number_of_users as int) as number_of_users,
    cast(i_18_locales as int) as i_18_locales,
    cast(i_18_content_types as int) as i_18_content_types,
    plugins,
    providers,
    license_type,
    cast(license_is_trial as boolean) as license_is_trial,
    project_type
    from distinct_data
)

select * from filtered_data