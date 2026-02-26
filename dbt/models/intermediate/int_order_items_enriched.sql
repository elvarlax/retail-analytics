-- Canonical line-item view. Joins order items to their parent order.
-- Derives order_date, is_completed, and carries order context down to the line grain.
-- fact_order_items is a simple projection over this model with no additional joins.
with order_items as (
    select * from {{ ref('stg_order_items') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
)
select
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    oi.product_id,

    -- line measures
    oi.quantity,
    oi.unit_price,
    oi.line_amount,
    o.total_amount as order_total_amount,

    -- derived
    o.created_at::date as order_date,
    (o.status = 'Completed') as is_completed,

    -- order context
    o.status,
    o.created_at as order_created_at,
    o.completed_at as order_completed_at

from order_items oi
join orders o using (order_id)
