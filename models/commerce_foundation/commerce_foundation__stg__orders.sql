{{
  config({    
    "materialized": "table",
    "alias": "commerce_foundation__stg__orders",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH stg_ord_source AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'orders') }}

),

stg_ord_typed AS (

  SELECT 
    CAST(order_id AS BIGINT) AS order_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(DATE(created_at) AS DATE) AS order_date,
    CAST(website_session_id AS BIGINT) AS website_session_id,
    CAST(user_id AS BIGINT) AS user_id,
    CAST(primary_product_id AS BIGINT) AS primary_product_id,
    CAST(items_purchased AS BIGINT) AS items_purchased,
    CAST(price_usd AS DOUBLE) AS price_usd,
    CAST(cogs_usd AS DOUBLE) AS cogs_usd,
    CAST(price_usd - cogs_usd AS DOUBLE) AS gross_margin_usd
  
  FROM stg_ord_source

)

SELECT *

FROM stg_ord_typed
