{{ config(materialized='table') }}

with payment_types as (

    select distinct
        payment_type
    from {{ ref('stg_order_payments') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['payment_type']) }} as payment_type_sk,
    payment_type

from payment_types
