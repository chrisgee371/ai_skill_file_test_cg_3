{{
  config({    
    "materialized": "table",
    "alias": "int__session_entry_page",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH int_sep_pageviews AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'website_pageviews') }}

),

int_sep_ranked AS (

  SELECT 
    website_session_id,
    website_pageview_id,
    pageview_url,
    created_at,
    ROW_NUMBER() OVER (PARTITION BY website_session_id ORDER BY created_at, website_pageview_id) AS page_rank
  
  FROM int_sep_pageviews

),

int_sep_first_page AS (

  SELECT * 
  
  FROM int_sep_ranked
  
  WHERE page_rank = 1

),

int_sep_final AS (

  SELECT 
    CAST(website_session_id AS BIGINT) AS website_session_id,
    CAST(pageview_url AS STRING) AS entry_page,
    CAST(website_pageview_id AS BIGINT) AS first_pageview_id,
    CAST(created_at AS TIMESTAMP) AS entry_page_created_at
  
  FROM int_sep_first_page

)

SELECT *

FROM int_sep_final
