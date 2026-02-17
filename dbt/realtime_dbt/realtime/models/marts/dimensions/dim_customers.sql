{{ config(materialized='table') }}

with customers as (

    select *
    from {{ ref('stg_customers') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state

from customers
