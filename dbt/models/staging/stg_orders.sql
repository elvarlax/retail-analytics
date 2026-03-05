with orders as (
    select * from {{ source('events', 'retail_events') }}
    where event_type = 'OrderPlacedV1'
),

status_changes as (
    select * from {{ source('events', 'retail_events') }}
    where event_type = 'OrderStatusChangedV1'
),

-- One row per order: the most recent status change wins.
latest_status as (
    select distinct on (payload ->> 'OrderId')
        (payload ->> 'OrderId')::uuid as order_id,
        payload ->> 'NewStatus'       as status,
        occurred_at_utc               as status_changed_at
    from status_changes
    order by payload ->> 'OrderId', occurred_at_utc desc
)

select
    (o.payload ->> 'OrderId')::uuid        as order_id,
    (o.payload ->> 'CustomerId')::uuid     as customer_id,
    coalesce(s.status, 'Pending')          as status,
    (o.payload ->> 'TotalAmount')::numeric as total_amount,
    o.occurred_at_utc                      as created_at,
    case when s.status = 'Completed' then s.status_changed_at end as completed_at
from orders o
left join latest_status s on (o.payload ->> 'OrderId')::uuid = s.order_id
