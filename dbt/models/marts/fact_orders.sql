-- Fact: orders (order grain)
-- One row per order. Use this fact for order-level aggregations:
-- average order value (AOV), order counts, conversion rates, fulfilment time.
-- total_amount is fully additive at this grain — safe to sum without deduplication.
-- For product-level or line-level analysis, use fact_order_items instead.
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='append_new_columns'
    )
}}
with enriched as (
    select * from {{ ref('int_orders_enriched') }}

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
    order_id,
    customer_id,
    order_date as date_key,

    -- measures
    total_amount,

    -- descriptors
    is_completed,
    status,
    order_month,
    order_created_at,
    order_completed_at

from enriched
