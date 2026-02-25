-- Staging: order_items
-- Renames id → order_item_id. Derives line_amount as the first computed
-- measure in the pipeline — quantity × unit_price at the time of the order.
-- unit_price is a snapshot of the product price when the order was placed
-- and is independent of the current price in dim_products.
with source as (
    select * from {{ source('raw', 'order_items') }}
)
select
    id as order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    -- Gross line revenue; fully additive across all dimensions
    quantity * unit_price as line_amount
from source
