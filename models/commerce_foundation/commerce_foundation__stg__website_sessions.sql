{{
  config({    
    "materialized": "table",
    "alias": "commerce_foundation__stg__website_sessions",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH stg_ws_source AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'website_sessions') }}

),

stg_ws_typed AS (

  SELECT 
    CAST(website_session_id AS BIGINT) AS website_session_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(DATE(created_at) AS DATE) AS session_date,
    CAST(user_id AS BIGINT) AS user_id,
    CAST(is_repeat_session AS BIGINT) AS is_repeat_session,
    CAST(utm_source AS STRING) AS utm_source,
    CAST(utm_campaign AS STRING) AS utm_campaign,
    CAST(utm_content AS STRING) AS utm_content,
    CAST(device_type AS STRING) AS device_type,
    CAST(http_referer AS STRING) AS http_referer
  
  FROM stg_ws_source

)

SELECT *

FROM stg_ws_typed
