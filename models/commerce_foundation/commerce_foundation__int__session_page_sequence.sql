{{
  config({    
    "materialized": "table",
    "alias": "int__session_page_sequence",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH int_sps_pageviews AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'website_pageviews') }}

),

int_sps_categorized AS (

  SELECT 
    website_session_id,
    website_pageview_id,
    created_at,
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
  
  FROM int_sps_pageviews

),

int_sps_sequenced AS (

  SELECT 
    website_session_id,
    website_pageview_id,
    created_at,
    pageview_url,
    page_category,
    ROW_NUMBER() OVER (PARTITION BY website_session_id ORDER BY created_at, website_pageview_id) AS page_sequence_number
  
  FROM int_sps_categorized

),

int_sps_final AS (

  SELECT 
    CAST(website_session_id AS BIGINT) AS website_session_id,
    CAST(page_sequence_number AS BIGINT) AS page_sequence_number,
    CAST(website_pageview_id AS BIGINT) AS website_pageview_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(pageview_url AS STRING) AS pageview_url,
    CAST(page_category AS STRING) AS page_category
  
  FROM int_sps_sequenced

)

SELECT *

FROM int_sps_final
