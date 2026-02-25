-- Order-level view. Joins orders with their customer.
-- Derives analytical convenience columns consumed by fact_orders.
with orders as (
    select * from {{ ref('stg_orders') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
)
select
    o.order_id,
    o.customer_id,
    c.full_name as customer_name,
    c.email,

    -- measures
    o.total_amount,

    -- derived
    o.created_at::date as order_date,
    date_trunc('month', o.created_at)::date as order_month,
    (o.status = 'Completed') as is_completed,

    -- descriptors
    o.status,
    o.created_at as order_created_at,
    o.completed_at as order_completed_at

from orders o
join customers c using (customer_id)
