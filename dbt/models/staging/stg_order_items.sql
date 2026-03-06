with orders as (
    select * from {{ source('events', 'retail_events') }}
    where event_type = 'OrderPlacedV1'
),

items as (
    select
        (payload ->> 'OrderId')::uuid   as order_id,
        (item ->> 'ProductId')::uuid    as product_id,
        (item ->> 'Quantity')::int      as quantity,
        (item ->> 'UnitPrice')::numeric as unit_price,
        row_number() over (
            partition by payload ->> 'OrderId'
            order by item ->> 'ProductId'
        ) as item_seq
    from orders,
    jsonb_array_elements(payload -> 'Items') as item
)

select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id', 'item_seq']) }} as order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    quantity * unit_price as line_amount
from items
