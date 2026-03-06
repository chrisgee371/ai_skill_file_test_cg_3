{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH pvd_bam_orders AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__stg__orders') }}

),

pvd_bam_order_metrics AS (

  SELECT 
    o.order_id,
    o.order_date,
    o.primary_product_id,
    o.items_purchased,
    o.price_usd AS order_price,
    CASE
      WHEN o.items_purchased > 1
        THEN 1
      ELSE 0
    END AS has_crosssell
  
  FROM pvd_bam_orders AS o

),

pvd_bam_order_items AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__stg__order_items') }}

),

pvd_bam_crosssell_items_source_table_000 AS (

  SELECT * 
  
  FROM pvd_bam_order_items

),

pvd_bam_crosssell_items_from_000 AS (

  SELECT 
    oi.order_id,
    oi.product_id AS crosssell_product_id,
    oi.price_usd AS crosssell_price,
    oi.is_primary_item
  
  FROM pvd_bam_crosssell_items_source_table_000 AS oi

),

pvd_bam_crosssell_items_filter_001 AS (

  SELECT * 
  
  FROM pvd_bam_crosssell_items_from_000
  
  WHERE is_primary_item = 0

),

pvd_bam_crosssell_items_projection_002 AS (

  SELECT 
    order_id,
    crosssell_product_id,
    crosssell_price
  
  FROM pvd_bam_crosssell_items_filter_001

),

pvd_bam_order_with_crosssell AS (

  SELECT 
    om.order_id,
    om.order_date,
    om.primary_product_id,
    om.items_purchased,
    om.order_price,
    om.has_crosssell,
    cs.crosssell_product_id,
    cs.crosssell_price
  
  FROM pvd_bam_order_metrics AS om
  LEFT JOIN pvd_bam_crosssell_items_projection_002 AS cs
     ON om.order_id = cs.order_id

),

pvd_bam_pair_aggregated AS (

  SELECT 
    order_date AS analysis_date,
    primary_product_id,
    crosssell_product_id,
    COUNT(DISTINCT order_id) AS pair_orders,
    SUM(COALESCE(crosssell_price, 0.0)) AS crosssell_revenue
  
  FROM pvd_bam_order_with_crosssell
  
  GROUP BY 
    order_date, primary_product_id, crosssell_product_id

),

pvd_bam_products AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__dim__products') }}

),

pvd_bam_with_names AS (

  SELECT 
    pa.analysis_date,
    pa.primary_product_id,
    pp.product_short_name AS primary_product_name,
    pa.crosssell_product_id,
    cp.product_short_name AS crosssell_product_name,
    pa.pair_orders,
    pa.crosssell_revenue
  
  FROM pvd_bam_pair_aggregated AS pa
  LEFT JOIN pvd_bam_products AS pp
     ON pa.primary_product_id = pp.product_id
  LEFT JOIN pvd_bam_products AS cp
     ON pa.crosssell_product_id = cp.product_id

),

pvd_bam_final AS (

  SELECT 
    CAST(analysis_date AS DATE) AS analysis_date,
    CAST(primary_product_id AS BIGINT) AS primary_product_id,
    CAST(primary_product_name AS STRING) AS primary_product_name,
    CAST(crosssell_product_id AS BIGINT) AS crosssell_product_id,
    CAST(crosssell_product_name AS STRING) AS crosssell_product_name,
    CAST(pair_orders AS BIGINT) AS pair_orders,
    CAST(crosssell_revenue AS DOUBLE) AS crosssell_revenue
  
  FROM pvd_bam_with_names

)

SELECT *

FROM pvd_bam_final
