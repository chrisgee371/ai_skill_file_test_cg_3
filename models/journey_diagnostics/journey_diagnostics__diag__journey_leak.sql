{{
  config({    
    "materialized": "table",
    "alias": "diag__journey_leak",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH djl_page_sequence AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'int__session_page_sequence') }}

),

djl_sessions AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'stg__website_sessions') }}

),

djl_page_with_step AS (

  SELECT 
    ps.website_session_id,
    s.session_date,
    ps.page_category,
    CASE
      WHEN ps.page_category IN ('lander', 'home')
        THEN 'landing'
      ELSE ps.page_category
    END AS funnel_step,
    CASE
      WHEN ps.page_category IN ('lander', 'home')
        THEN 1
      WHEN ps.page_category = 'products'
        THEN 2
      WHEN ps.page_category = 'product_detail'
        THEN 3
      WHEN ps.page_category = 'cart'
        THEN 4
      WHEN ps.page_category = 'shipping'
        THEN 5
      WHEN ps.page_category = 'billing'
        THEN 6
      WHEN ps.page_category = 'thank_you'
        THEN 7
      ELSE 0
    END AS step_order
  
  FROM djl_page_sequence AS ps
  INNER JOIN djl_sessions AS s
     ON ps.website_session_id = s.website_session_id

),

djl_valid_steps AS (

  SELECT * 
  
  FROM djl_page_with_step
  
  WHERE step_order > 0

),

djl_session_furthest AS (

  SELECT 
    website_session_id,
    session_date,
    MAX(step_order) AS furthest_step_order
  
  FROM djl_valid_steps
  
  GROUP BY 
    website_session_id, session_date

),

djl_step5_counts AS (

  SELECT 
    session_date AS analysis_date,
    'funnel_step' AS entity_type,
    'shipping' AS entity_key,
    5 AS step_order,
    COUNT(CASE
      WHEN furthest_step_order >= 5
        THEN website_session_id
    END) AS sessions_entering,
    COUNT(CASE
      WHEN furthest_step_order > 5
        THEN website_session_id
    END) AS sessions_continuing
  
  FROM djl_session_furthest
  
  GROUP BY session_date

),

djl_step6_counts AS (

  SELECT 
    session_date AS analysis_date,
    'funnel_step' AS entity_type,
    'billing' AS entity_key,
    6 AS step_order,
    COUNT(CASE
      WHEN furthest_step_order >= 6
        THEN website_session_id
    END) AS sessions_entering,
    COUNT(CASE
      WHEN furthest_step_order > 6
        THEN website_session_id
    END) AS sessions_continuing
  
  FROM djl_session_furthest
  
  GROUP BY session_date

),

djl_step2_counts AS (

  SELECT 
    session_date AS analysis_date,
    'funnel_step' AS entity_type,
    'products' AS entity_key,
    2 AS step_order,
    COUNT(CASE
      WHEN furthest_step_order >= 2
        THEN website_session_id
    END) AS sessions_entering,
    COUNT(CASE
      WHEN furthest_step_order > 2
        THEN website_session_id
    END) AS sessions_continuing
  
  FROM djl_session_furthest
  
  GROUP BY session_date

),

djl_step3_counts AS (

  SELECT 
    session_date AS analysis_date,
    'funnel_step' AS entity_type,
    'product_detail' AS entity_key,
    3 AS step_order,
    COUNT(CASE
      WHEN furthest_step_order >= 3
        THEN website_session_id
    END) AS sessions_entering,
    COUNT(CASE
      WHEN furthest_step_order > 3
        THEN website_session_id
    END) AS sessions_continuing
  
  FROM djl_session_furthest
  
  GROUP BY session_date

),

djl_step4_counts AS (

  SELECT 
    session_date AS analysis_date,
    'funnel_step' AS entity_type,
    'cart' AS entity_key,
    4 AS step_order,
    COUNT(CASE
      WHEN furthest_step_order >= 4
        THEN website_session_id
    END) AS sessions_entering,
    COUNT(CASE
      WHEN furthest_step_order > 4
        THEN website_session_id
    END) AS sessions_continuing
  
  FROM djl_session_furthest
  
  GROUP BY session_date

),

djl_step1_counts AS (

  SELECT 
    session_date AS analysis_date,
    'funnel_step' AS entity_type,
    'landing' AS entity_key,
    1 AS step_order,
    COUNT(CASE
      WHEN furthest_step_order >= 1
        THEN website_session_id
    END) AS sessions_entering,
    COUNT(CASE
      WHEN furthest_step_order > 1
        THEN website_session_id
    END) AS sessions_continuing
  
  FROM djl_session_furthest
  
  GROUP BY session_date

),

djl_all_steps AS (

  SELECT * 
  
  FROM djl_step1_counts
  
  UNION ALL
  
  SELECT * 
  
  FROM djl_step2_counts
  
  UNION ALL
  
  SELECT * 
  
  FROM djl_step3_counts
  
  UNION ALL
  
  SELECT * 
  
  FROM djl_step4_counts
  
  UNION ALL
  
  SELECT * 
  
  FROM djl_step5_counts
  
  UNION ALL
  
  SELECT * 
  
  FROM djl_step6_counts

),

djl_billing_completion_source_table_000 AS (

  SELECT * 
  
  FROM djl_all_steps

),

djl_billing_completion_from_000 AS (

  SELECT 
    analysis_date,
    sessions_entering AS billing_sessions,
    sessions_continuing AS completed_sessions,
    entity_key
  
  FROM djl_billing_completion_source_table_000

),

djl_billing_completion_filter_001 AS (

  SELECT * 
  
  FROM djl_billing_completion_from_000
  
  WHERE entity_key = 'billing'

),

djl_billing_completion_projection_002 AS (

  SELECT 
    analysis_date,
    billing_sessions,
    completed_sessions
  
  FROM djl_billing_completion_filter_001

),

djl_enriched AS (

  SELECT 
    a.analysis_date,
    a.entity_type,
    a.entity_key,
    a.sessions_entering,
    a.sessions_continuing,
    a.sessions_entering - a.sessions_continuing AS sessions_dropped,
    CAST(bc.completed_sessions AS DOUBLE) / NULLIF(bc.billing_sessions, 0) AS checkout_completion_rate,
    a.step_order
  
  FROM djl_all_steps AS a
  LEFT JOIN djl_billing_completion_projection_002 AS bc
     ON a.analysis_date = bc.analysis_date

),

djl_scored AS (

  SELECT 
    analysis_date,
    entity_type,
    entity_key,
    sessions_entering,
    sessions_continuing,
    sessions_dropped,
    checkout_completion_rate,
    CASE
      WHEN sessions_entering < 10
        THEN 0.0
      ELSE LEAST(
        100.0, 
        GREATEST(0.0, (step_order * 10.0) + (CAST(sessions_dropped AS DOUBLE) / NULLIF(sessions_entering, 0) * 50.0)))
    END AS severity_score
  
  FROM djl_enriched

),

djl_final AS (

  SELECT 
    CAST(analysis_date AS DATE) AS analysis_date,
    CAST(entity_type AS STRING) AS entity_type,
    CAST(entity_key AS STRING) AS entity_key,
    CAST(sessions_entering AS BIGINT) AS sessions_entering,
    CAST(sessions_continuing AS BIGINT) AS sessions_continuing,
    CAST(sessions_dropped AS BIGINT) AS sessions_dropped,
    CAST(checkout_completion_rate AS DOUBLE) AS checkout_completion_rate,
    CAST(severity_score AS DOUBLE) AS severity_score,
    CAST(CASE
      WHEN severity_score >= 70
        THEN 'Critical funnel leak - immediate UX review needed'
      WHEN severity_score >= 50
        THEN 'Significant dropoff - test page improvements'
      WHEN severity_score >= 30
        THEN 'Monitor dropoff trends'
      ELSE NULL
    END AS STRING) AS recommended_action
  
  FROM djl_scored

)

SELECT *

FROM djl_final
