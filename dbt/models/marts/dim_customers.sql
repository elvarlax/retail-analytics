-- Dimension: customers
-- One row per customer. Simple projection from stg_customers with no
-- additional transformations — all renaming and derivation happened in staging.
-- Join to fact_order_items or fact_orders on customer_id.
with customers as (
    select * from {{ ref('stg_customers') }}
)
select
    customer_id,
    first_name,
    last_name,
    full_name,
    email
from customers
