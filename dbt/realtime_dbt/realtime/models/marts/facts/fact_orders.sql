{{ config(materialized='table') }}

with order_items as (

    select *
    from {{ ref('stg_order_items') }}

),

orders as (

    select *
    from {{ ref('stg_orders') }}

),

payments as (

    select
        order_id,
        sum(payment_value) as total_payment_value
    from {{ ref('stg_order_payments') }}
    group by order_id

),

joined as (

    select
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        oi.price,
        oi.freight_value,
        p.total_payment_value

    from order_items oi
    left join orders o
        on oi.order_id = o.order_id
    left join payments p
        on oi.order_id = p.order_id

),

final as (

    select
        -- Surrogate Keys
        {{ dbt_utils.generate_surrogate_key(['j.order_id','j.order_item_id']) }} as order_item_sk,

        dc.customer_sk,
        dp.product_sk,
        ds.seller_sk,
        dd.date_sk,

        -- Natural Keys
        j.order_id,
        j.order_item_id,

        -- Métricas financeiras
        j.price,
        j.freight_value,
        j.price + j.freight_value as gross_item_value,

        -- Métricas derivadas
        j.total_payment_value,

        -- Métricas logísticas
        j.order_status,

        j.order_purchase_timestamp,
        j.order_delivered_customer_date,
        j.order_estimated_delivery_date,

        -- Lead time (dias)
        case 
            when j.order_delivered_customer_date is not null
            then (j.order_delivered_customer_date::date - j.order_purchase_timestamp::date)
            else null
        end as delivery_lead_time_days,

        -- SLA delay
        case
            when j.order_delivered_customer_date > j.order_estimated_delivery_date
            then true
            else false
        end as is_late_delivery

    from joined j

    left join {{ ref('dim_customers') }} dc
        on j.customer_id = dc.customer_id

    left join {{ ref('dim_products') }} dp
        on j.product_id = dp.product_id

    left join {{ ref('dim_sellers') }} ds
        on j.seller_id = ds.seller_id

    left join {{ ref('dim_date') }} dd
        on date(j.order_purchase_timestamp) = dd.full_date

)

select * from final
