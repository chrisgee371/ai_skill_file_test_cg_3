{{
  config({    
    "materialized": "table",
    "alias": "commerce_foundation__dim__products",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH dim_prod_source AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'products') }}

),

dim_prod_typed AS (

  SELECT 
    CAST(product_id AS BIGINT) AS product_id,
    CAST(created_at AS TIMESTAMP) AS product_launch_timestamp,
    CAST(DATE(created_at) AS DATE) AS product_launch_date,
    CAST(product_name AS STRING) AS product_name,
    CAST(CASE
      WHEN product_name = 'The Original Mr. Fuzzy'
        THEN 'Mr. Fuzzy'
      WHEN product_name = 'The Forever Love Bear'
        THEN 'Love Bear'
      WHEN product_name = 'The Birthday Sugar Panda'
        THEN 'Sugar Panda'
      WHEN product_name = 'The Hudson River Mini Bear'
        THEN 'Mini Bear'
      ELSE SUBSTRING(product_name, 1, 12)
    END AS STRING) AS product_short_name
  
  FROM dim_prod_source

)

SELECT *

FROM dim_prod_typed
