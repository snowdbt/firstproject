
{{ config(materialized='incremental', unique_key='user_id') }}

with users_aggregated as (

    select
        user_id,
        min(event_time) as first_event_time,
        max(event_time) as last_event_time,
        count(*) as count_total_events

    from {{ ref('users') }}
    group by 1

)

select *,
    -- Inject the run id if present, otherwise use "manual"
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _audit_run_id

from users_aggregated