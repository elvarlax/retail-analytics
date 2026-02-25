-- Dimension: products
-- One row per product. stock_quantity is intentionally excluded — inventory
-- levels change continuously and belong in a separate inventory fact, not in
-- a dimension. The unit_price here reflects the current list price; note that
-- fact_order_items.unit_price is a snapshot of the price at time of order.
-- Join to fact_order_items on product_id.
with products as (
    select * from {{ ref('stg_products') }}
)
select
    product_id,
    product_name,
    sku,
    unit_price
from products
