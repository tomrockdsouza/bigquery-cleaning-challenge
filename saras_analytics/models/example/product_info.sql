
-- models/product_info.sql

with products as (
    select distinct sku
    from {{ source('saras_analytics', 'products') }}
),
     warehouse_products as (
         select product_id, count(1) as count_wp
         from {{ source('saras_analytics', 'warehouse_products') }}
         group by product_id
     ),
     vendors as (
         select product_id, count(1) as count_v
         from {{ source('saras_analytics', 'vendors') }}
         group by product_id
     )

select
    p.sku,
    wp.count_wp,
    v.count_v
from
    products p
    left join
    warehouse_products wp
    on
    p.sku = wp.product_id
    left join
    vendors v
    on
    p.sku = v.product_id
