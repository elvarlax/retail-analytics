-- stock_quantity excluded — inventory levels change continuously and belong
-- in an inventory fact, not a dimension.
with products as (
    select * from {{ ref('stg_products') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id,
    product_name,
    sku,
    unit_price,
    image_url
from products
