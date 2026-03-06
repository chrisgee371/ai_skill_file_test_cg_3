{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH src_ord_raw AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'orders') }}

),

src_ord_typed AS (

  SELECT 
    CAST(order_id AS BIGINT) AS order_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(website_session_id AS BIGINT) AS website_session_id,
    CAST(user_id AS BIGINT) AS user_id,
    CAST(primary_product_id AS BIGINT) AS primary_product_id,
    CAST(items_purchased AS BIGINT) AS items_purchased,
    CAST(price_usd AS DOUBLE) AS price_usd,
    CAST(cogs_usd AS DOUBLE) AS cogs_usd
  
  FROM src_ord_raw

)

SELECT *

FROM src_ord_typed
