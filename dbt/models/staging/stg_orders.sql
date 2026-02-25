-- Staging: orders
-- Renames id → order_id. Maps the source status integer to a human-readable
-- string. This is the only place in the pipeline where the integer encoding
-- is translated — all downstream models work with the text values.
with source as (
    select * from {{ source('raw', 'orders') }}
)
select
    id as order_id,
    customer_id,
    -- Source encodes status as an integer: 0=Pending, 1=Completed, 2=Cancelled
    case status
        when 0 then 'Pending'
        when 1 then 'Completed'
        when 2 then 'Cancelled'
        else 'Unknown'
    end as status,
    total_amount,
    created_at,
    completed_at
from source
