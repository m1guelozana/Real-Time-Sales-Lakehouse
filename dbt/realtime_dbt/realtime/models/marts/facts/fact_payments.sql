{{ config(materialized='table') }}

with payments as (

    select *
    from {{ ref('stg_order_payments') }}

),

orders as (

    select *
    from {{ ref('stg_orders') }}

),

joined as (

    select
        p.order_id,
        p.payment_sequential,
        p.payment_type,
        p.payment_installments,
        p.payment_value,
        o.customer_id,
        o.order_purchase_timestamp

    from payments p
    left join orders o
        on p.order_id = o.order_id

),

final as (

    select
        -- Surrogate key da fact
        {{ dbt_utils.generate_surrogate_key(['j.order_id','j.payment_sequential']) }} as payment_sk,

        -- Dimensões
        dc.customer_sk,
        dd.date_sk,
        dpt.payment_type_sk,

        -- Natural keys
        j.order_id,
        j.payment_sequential,

        -- Métricas
        j.payment_installments,
        j.payment_value

    from joined j

    left join {{ ref('dim_customers') }} dc
        on j.customer_id = dc.customer_id

    left join {{ ref('dim_payment_type') }} dpt
        on j.payment_type = dpt.payment_type

    left join {{ ref('dim_date') }} dd
        on date(j.order_purchase_timestamp) = dd.full_date

)

select * from final
