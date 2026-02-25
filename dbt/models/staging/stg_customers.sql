-- Staging: customers
-- Renames the source PK (id → customer_id) and derives full_name.
-- No business logic — pure renaming and casting only.
with source as (
    select * from {{ source('raw', 'customers') }}
)
select
    id as customer_id,
    first_name,
    last_name,
    -- Convenience display name used in reporting; not a natural key
    first_name || ' ' || last_name as full_name,
    email
from source
