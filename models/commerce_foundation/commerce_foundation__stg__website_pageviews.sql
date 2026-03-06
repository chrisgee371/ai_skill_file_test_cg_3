{{
  config({    
    "materialized": "table",
    "alias": "stg__website_pageviews",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH stg_pv_source AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'website_pageviews') }}

),

stg_pv_categorized AS (

  SELECT 
    website_pageview_id,
    created_at,
    website_session_id,
    pageview_url,
    CASE
      WHEN pageview_url = '/home'
        THEN 'home'
      WHEN pageview_url LIKE '/lander-%'
        THEN 'lander'
      WHEN pageview_url = '/products'
        THEN 'products'
      WHEN pageview_url LIKE '/the-%'
        THEN 'product_detail'
      WHEN pageview_url = '/cart'
        THEN 'cart'
      WHEN pageview_url = '/shipping'
        THEN 'shipping'
      WHEN pageview_url LIKE '/billing%'
        THEN 'billing'
      WHEN pageview_url = '/thank-you-for-your-order'
        THEN 'thank_you'
      ELSE 'other'
    END AS page_category
  
  FROM stg_pv_source

),

stg_pv_final AS (

  SELECT 
    CAST(website_pageview_id AS BIGINT) AS website_pageview_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(DATE(created_at) AS DATE) AS page_date,
    CAST(website_session_id AS BIGINT) AS website_session_id,
    CAST(pageview_url AS STRING) AS pageview_url,
    CAST(page_category AS STRING) AS page_category
  
  FROM stg_pv_categorized

)

SELECT *

FROM stg_pv_final
