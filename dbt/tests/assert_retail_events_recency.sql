-- Freshness guard for event ingestion.
-- If events exist, the most recent received_at must be within the configured window.
-- Defaults to 168 hours (7 days) and can be overridden via:
--   dbt build --vars '{events_recency_hours: 24}'
-- Returns rows on failure.
with stats as (
    select
        count(*) as event_count,
        max(received_at) as latest_received_at
    from {{ source('events', 'retail_events') }}
)

select
    event_count,
    latest_received_at
from stats
where event_count > 0
  and latest_received_at < now() - make_interval(hours => {{ var('events_recency_hours', 168) | int }})
