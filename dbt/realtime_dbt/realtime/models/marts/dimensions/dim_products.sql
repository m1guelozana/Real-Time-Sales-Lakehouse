{{ config(materialized='table') }}

with products as (

    select *
    from {{ ref('stg_products') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm

from products
