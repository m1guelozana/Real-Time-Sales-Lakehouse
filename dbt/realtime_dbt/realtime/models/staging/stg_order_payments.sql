{{ config(materialized='view') }}

select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
from {{ source('bronze', 'order_payments') }}
