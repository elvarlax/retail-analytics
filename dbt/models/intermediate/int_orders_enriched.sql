with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    order_id,
    customer_id,
    total_amount,
    created_at::date                      as order_date,
    date_trunc('month', created_at)::date as order_month,
    (status = 'Completed')                as is_completed,
    status,
    created_at                            as order_created_at,
    completed_at                          as order_completed_at
from orders
