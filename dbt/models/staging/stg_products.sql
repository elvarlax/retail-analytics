with source as (
    select * from {{ source('events', 'retail_events') }}
    where event_type = 'ProductCreatedV1'
)

select
    (payload ->> 'ProductId')::uuid    as product_id,
    payload ->> 'Name'                 as product_name,
    payload ->> 'SKU'                  as sku,
    (payload ->> 'Price')::numeric     as unit_price,
    (payload ->> 'StockQuantity')::int as stock_quantity,
    payload ->> 'ImageUrl'             as image_url,
    occurred_at_utc                    as created_at
from source
