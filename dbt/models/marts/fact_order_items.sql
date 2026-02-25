-- Fact: order_items (line grain)
-- One row per order line item. Use this fact for product-level analysis:
-- revenue by product, units sold, customer lifetime value by line.
-- For order-level aggregations (AOV, order counts), use fact_orders instead.
--
-- order_total_amount is denormalised here for convenience but is semi-additive:
-- it double-counts if summed across multiple line items on the same order.
-- Always deduplicate by order_id before summing it.
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
    -- New orders created since last run, plus any orders that changed status
    -- (Pending → Completed) since the last completed_at we've seen.
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
)
select
    order_item_id,
    order_id,
    customer_id,
    product_id,
    order_date as date_key,

    -- measures
    quantity,
    unit_price,
    line_amount,
    order_total_amount,

    -- descriptors
    is_completed,
    status,
    order_created_at,
    order_completed_at

from enriched
