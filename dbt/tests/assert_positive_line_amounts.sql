-- Every order line item must have a positive line amount.
-- A zero or negative value indicates a data integrity problem
-- (e.g. zero quantity or negative unit price).
-- Returns rows on failure.
select
    order_item_id,
    quantity,
    unit_price,
    line_amount
from {{ ref('fact_order_items') }}
where line_amount <= 0
