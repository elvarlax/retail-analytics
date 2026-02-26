-- Order-level view. Derives analytical convenience columns consumed by fact_orders.
-- Consumers join to dim_customers on customer_id for customer attributes.
with orders as (
    select * from {{ ref('stg_orders') }}
)
select
    order_id,
    customer_id,

    -- measures
    total_amount,

    -- derived
    created_at::date as order_date,
    date_trunc('month', created_at)::date as order_month,
    (status = 'Completed') as is_completed,

    -- descriptors
    status,
    created_at as order_created_at,
    completed_at as order_completed_at

from orders
