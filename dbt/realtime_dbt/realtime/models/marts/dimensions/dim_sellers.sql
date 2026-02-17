{{ config(materialized='table') }}

with sellers as (

    select *
    from {{ ref('stg_sellers') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_sk,
    seller_id,
    seller_city,
    seller_state

from sellers
