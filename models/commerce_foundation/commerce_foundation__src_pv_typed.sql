{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH src_pv_raw AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'website_pageviews') }}

),

src_pv_typed AS (

  SELECT 
    CAST(website_pageview_id AS BIGINT) AS website_pageview_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(website_session_id AS BIGINT) AS website_session_id,
    CAST(pageview_url AS STRING) AS pageview_url
  
  FROM src_pv_raw

)

SELECT *

FROM src_pv_typed
