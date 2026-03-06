{{
  config({    
    "materialized": "table",
    "alias": "commerce_foundation__stg__order_items",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH stg_oi_source AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'order_items') }}

),

stg_oi_typed AS (

  SELECT 
    CAST(order_item_id AS BIGINT) AS order_item_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(DATE(created_at) AS DATE) AS order_item_date,
    CAST(order_id AS BIGINT) AS order_id,
    CAST(product_id AS BIGINT) AS product_id,
    CAST(is_primary_item AS BIGINT) AS is_primary_item,
    CAST(CASE
      WHEN is_primary_item = 1
        THEN TRUE
      ELSE FALSE
    END AS BOOLEAN) AS is_primary_item_flag,
    CAST(price_usd AS DOUBLE) AS price_usd,
    CAST(cogs_usd AS DOUBLE) AS cogs_usd,
    CAST(price_usd - cogs_usd AS DOUBLE) AS gross_margin_usd
  
  FROM stg_oi_source

)

SELECT *

FROM stg_oi_typed
