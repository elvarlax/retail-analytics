-- Date spine from the earliest order date to today.
-- COALESCE guards against an empty orders table on first run.
with date_spine as (
    select generate_series(
        coalesce(
            (select min(created_at)::date from {{ ref('stg_orders') }}),
            current_date - interval '1 year'
        ),
        current_date,
        interval '1 day'
    )::date as date_day
)
select
    date_day                                  as date_key,
    extract(year    from date_day)::int       as year,
    extract(quarter from date_day)::int       as quarter,
    extract(month   from date_day)::int       as month,
    to_char(date_day, 'Month')                as month_name,
    extract(week    from date_day)::int       as week_of_year,
    extract(day     from date_day)::int       as day,
    extract(dow     from date_day)::int       as day_of_week,
    to_char(date_day, 'Day')                  as day_name,
    (extract(dow    from date_day) in (0, 6)) as is_weekend
from date_spine