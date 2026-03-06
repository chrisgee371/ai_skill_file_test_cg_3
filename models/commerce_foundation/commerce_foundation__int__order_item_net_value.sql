{{
  config({    
    "materialized": "table",
    "alias": "commerce_foundation__int__order_item_net_value",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH int_oinv_items AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'order_items') }}

),

int_oinv_refunds AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'order_item_refunds') }}

),

int_oinv_joined AS (

  SELECT 
    oi.order_item_id,
    oi.created_at AS order_item_created_at,
    DATE(oi.created_at) AS order_item_date,
    oi.order_id,
    oi.product_id,
    oi.is_primary_item,
    oi.price_usd,
    oi.cogs_usd,
    oi.price_usd - oi.cogs_usd AS gross_margin_usd,
    r.refund_amount_usd,
    r.created_at AS refund_created_at,
    CASE
      WHEN r.order_item_refund_id IS NOT NULL
        THEN TRUE
      ELSE FALSE
    END AS is_refunded,
    oi.price_usd - COALESCE(r.refund_amount_usd, 0.0) AS net_revenue_usd,
    (oi.price_usd - oi.cogs_usd) - COALESCE(r.refund_amount_usd, 0.0) AS net_margin_usd
  
  FROM int_oinv_items AS oi
  LEFT JOIN int_oinv_refunds AS r
     ON oi.order_item_id = r.order_item_id

),

int_oinv_final AS (

  SELECT 
    CAST(order_item_id AS BIGINT) AS order_item_id,
    CAST(order_item_created_at AS TIMESTAMP) AS order_item_created_at,
    CAST(order_item_date AS DATE) AS order_item_date,
    CAST(order_id AS BIGINT) AS order_id,
    CAST(product_id AS BIGINT) AS product_id,
    CAST(is_primary_item AS BIGINT) AS is_primary_item,
    CAST(price_usd AS DOUBLE) AS price_usd,
    CAST(cogs_usd AS DOUBLE) AS cogs_usd,
    CAST(gross_margin_usd AS DOUBLE) AS gross_margin_usd,
    CAST(refund_amount_usd AS DOUBLE) AS refund_amount_usd,
    CAST(refund_created_at AS TIMESTAMP) AS refund_created_at,
    CAST(is_refunded AS BOOLEAN) AS is_refunded,
    CAST(net_revenue_usd AS DOUBLE) AS net_revenue_usd,
    CAST(net_margin_usd AS DOUBLE) AS net_margin_usd
  
  FROM int_oinv_joined

)

SELECT *

FROM int_oinv_final
