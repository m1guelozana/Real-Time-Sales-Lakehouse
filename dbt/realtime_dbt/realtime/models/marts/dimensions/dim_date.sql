{{ config(materialized='table') }}

with dates as (

    select distinct
        date(order_purchase_timestamp) as full_date
    from {{ ref('stg_orders') }}

)

select
    to_char(full_date, 'YYYYMMDD')::int as date_sk,
    full_date,
    extract(year from full_date) as year,
    extract(month from full_date) as month,
    extract(day from full_date) as day,
    extract(quarter from full_date) as quarter,
    extract(dow from full_date) as day_of_week,
    extract(week from full_date) as week_of_year,
    case 
        when extract(dow from full_date) in (0,6) then true
        else false
    end as is_weekend

from dates
order by full_date
