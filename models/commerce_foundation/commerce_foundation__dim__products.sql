{{
  config({    
    "materialized": "table",
    "alias": "dim__products",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH dim_prod_source AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'products') }}

),

dim_prod_final AS (

  SELECT 
    CAST(product_id AS BIGINT) AS product_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(product_name AS STRING) AS product_name
  
  FROM dim_prod_source

)

SELECT *

FROM dim_prod_final
