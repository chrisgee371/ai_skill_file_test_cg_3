{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH pvd_rpm_net_value AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__order_item_net_value') }}

),

pvd_rpm_products AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__dim__products') }}

),

pvd_rpm_item_with_product AS (

  SELECT 
    nv.order_item_id,
    nv.order_item_date,
    nv.product_id,
    nv.price_usd,
    nv.refund_amount_usd,
    nv.is_refunded,
    nv.net_revenue_usd,
    p.product_name,
    p.product_short_name
  
  FROM pvd_rpm_net_value AS nv
  INNER JOIN pvd_rpm_products AS p
     ON nv.product_id = p.product_id

),

pvd_rpm_aggregated AS (

  SELECT 
    order_item_date AS analysis_date,
    product_id,
    product_name,
    product_short_name,
    COUNT(*) AS items_sold,
    SUM(price_usd) AS total_gross_revenue,
    SUM(CASE
      WHEN is_refunded
        THEN 1
      ELSE 0
    END) AS refunded_items,
    SUM(COALESCE(refund_amount_usd, 0.0)) AS total_refund_amount,
    SUM(net_revenue_usd) AS total_net_revenue
  
  FROM pvd_rpm_item_with_product
  
  GROUP BY 
    order_item_date, product_id, product_name, product_short_name

),

pvd_rpm_final AS (

  SELECT 
    CAST(analysis_date AS DATE) AS analysis_date,
    CAST(product_id AS BIGINT) AS product_id,
    CAST(product_name AS STRING) AS product_name,
    CAST(product_short_name AS STRING) AS product_short_name,
    CAST(items_sold AS BIGINT) AS items_sold,
    CAST(refunded_items AS BIGINT) AS refunded_items,
    CAST(total_gross_revenue AS DOUBLE) AS total_gross_revenue,
    CAST(total_refund_amount AS DOUBLE) AS total_refund_amount,
    CAST(total_net_revenue AS DOUBLE) AS total_net_revenue,
    CAST(CASE
      WHEN items_sold > 0
        THEN (refunded_items * 1.0) / items_sold
      ELSE 0.0
    END AS DOUBLE) AS refund_rate,
    CAST(CASE
      WHEN total_gross_revenue > 0
        THEN total_refund_amount / total_gross_revenue
      ELSE 0.0
    END AS DOUBLE) AS refund_value_rate
  
  FROM pvd_rpm_aggregated

)

SELECT *

FROM pvd_rpm_final
