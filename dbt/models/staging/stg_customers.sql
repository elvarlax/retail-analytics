with source as (
    select * from {{ source('events', 'retail_events') }}
    where event_type = 'CustomerCreatedV1'
)

select
    (payload ->> 'CustomerId')::uuid as customer_id,
    payload ->> 'FirstName'          as first_name,
    payload ->> 'LastName'           as last_name,
    (payload ->> 'FirstName') || ' ' || (payload ->> 'LastName') as full_name,
    payload ->> 'Email'              as email,
    occurred_at_utc                  as created_at
from source
