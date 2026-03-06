{{
  config({    
    "materialized": "table",
    "alias": "commerce_foundation__stg__website_pageviews",
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
    CAST(website_pageview_id AS BIGINT) AS website_pageview_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(DATE(created_at) AS DATE) AS page_date,
    CAST(website_session_id AS BIGINT) AS website_session_id,
    CAST(pageview_url AS STRING) AS pageview_url,
    CAST(CASE
      WHEN pageview_url = '/home'
        THEN 'home'
      WHEN pageview_url = '/lander-1'
        THEN 'lander'
      WHEN pageview_url = '/lander-2'
        THEN 'lander'
      WHEN pageview_url = '/lander-3'
        THEN 'lander'
      WHEN pageview_url = '/lander-4'
        THEN 'lander'
      WHEN pageview_url = '/lander-5'
        THEN 'lander'
      WHEN pageview_url = '/products'
        THEN 'products'
      WHEN pageview_url LIKE '/the-%'
        THEN 'product_detail'
      WHEN pageview_url = '/cart'
        THEN 'cart'
      WHEN pageview_url = '/shipping'
        THEN 'shipping'
      WHEN pageview_url = '/billing'
        THEN 'billing'
      WHEN pageview_url = '/billing-2'
        THEN 'billing'
      WHEN pageview_url = '/thank-you-for-your-order'
        THEN 'thankyou'
      ELSE 'other'
    END AS STRING) AS page_category
  
  FROM stg_pv_source

)

SELECT *

FROM stg_pv_categorized
