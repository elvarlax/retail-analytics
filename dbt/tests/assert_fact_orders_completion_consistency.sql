-- Completed orders must be marked completed and have a valid completion timestamp.
-- Non-completed orders must not be marked completed.
-- Returns rows on failure.
select
    order_id,
    status,
    is_completed,
    order_created_at,
    order_completed_at
from {{ ref('fact_orders') }}
where
    (
        status = 'Completed'
        and (
            is_completed is distinct from true
            or order_completed_at is null
            or order_completed_at < order_created_at
        )
    )
    or (
        status <> 'Completed'
        and is_completed is distinct from false
    )
