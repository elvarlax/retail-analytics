-- Every Completed order must have a completed_at timestamp
-- that is on or after its created_at timestamp.
-- Returns rows on failure.
select
    order_id,
    created_at,
    completed_at
from {{ ref('stg_orders') }}
where status = 'Completed'
and (completed_at is null or completed_at < created_at)