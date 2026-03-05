-- order_total_amount is semi-additive: summing it across line items on the
-- same order double-counts. Deduplicate by order_id before summing.
{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        on_schema_change='append_new_columns'
    )
}}

with enriched as (
    select * from {{ ref('int_order_items_enriched') }}

    {% if is_incremental() %}
    -- Picks up new orders and orders whose status changed since last run.
    where order_created_at > (select max(order_created_at) from {{ this }})
       or (
           order_completed_at is not null
           and order_completed_at > (
               select coalesce(max(order_completed_at), '1900-01-01')
               from {{ this }}
               where order_completed_at is not null
           )
       )
    {% endif %}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

products as (
    select * from {{ ref('dim_products') }}
)

select
    e.order_item_id,
    e.order_id,
    c.customer_key,
    p.product_key,
    e.order_date as date_key,
    e.quantity,
    e.unit_price,
    e.line_amount,
    e.order_total_amount,
    e.is_completed,
    e.status,
    e.order_created_at,
    e.order_completed_at
from enriched e
join customers c on e.customer_id = c.customer_id
join products p on e.product_id = p.product_id
