-- Staging: products
-- Renames source columns to semantic names (id → product_id, name → product_name,
-- price → unit_price). stock_quantity is carried through staging but intentionally
-- excluded from dim_products — it is an operational metric that changes continuously
-- and does not belong in a slowly changing dimension.
with source as (
    select * from {{ source('raw', 'products') }}
)
select
    id as product_id,
    name as product_name,
    sku,
    price as unit_price,
    stock_quantity
from source
