{{
  config({    
    "materialized": "table",
    "alias": "journey_diagnostics__diag__experiment_findings",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH jd_exp_pageviews AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__stg__website_pageviews') }}

),

jd_exp_session_pagecount AS (

  SELECT 
    website_session_id,
    COUNT(*) AS pageview_count
  
  FROM jd_exp_pageviews
  
  GROUP BY website_session_id

),

jd_exp_sequence AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__session_page_sequence') }}

),

jd_exp_billing_sessions_source_table_000 AS (

  SELECT * 
  
  FROM jd_exp_sequence

),

jd_exp_billing_sessions_from_000 AS (

  SELECT 
    website_session_id,
    pageview_url AS billing_variant,
    pageview_url
  
  FROM jd_exp_billing_sessions_source_table_000

),

jd_exp_billing_sessions_filter_001 AS (

  SELECT * 
  
  FROM jd_exp_billing_sessions_from_000
  
  WHERE pageview_url IN ('/billing', '/billing-2')

),

jd_exp_billing_sessions_projection_002 AS (

  SELECT 
    DISTINCT website_session_id,
    billing_variant
  
  FROM jd_exp_billing_sessions_filter_001

),

jd_exp_sessions AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__stg__website_sessions') }}

),

jd_exp_orders AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__session_order_bridge') }}

),

jd_exp_billing_with_metrics AS (

  SELECT 
    b.billing_variant,
    s.session_date,
    s.website_session_id,
    COALESCE(o.ordered_flag, 0) AS ordered_flag,
    COALESCE(pc.pageview_count, 1) AS pageview_count
  
  FROM jd_exp_sessions AS s
  INNER JOIN jd_exp_billing_sessions_projection_002 AS b
     ON s.website_session_id = b.website_session_id
  LEFT JOIN jd_exp_orders AS o
     ON s.website_session_id = o.website_session_id
  LEFT JOIN jd_exp_session_pagecount AS pc
     ON s.website_session_id = pc.website_session_id

),

jd_exp_billing_v1_bounds_source_table_000 AS (

  SELECT * 
  
  FROM jd_exp_billing_with_metrics

),

jd_exp_billing_v1_bounds_from_000 AS (

  SELECT 
    session_date,
    billing_variant
  
  FROM jd_exp_billing_v1_bounds_source_table_000

),

jd_exp_billing_v1_bounds_filter_001 AS (

  SELECT * 
  
  FROM jd_exp_billing_v1_bounds_from_000
  
  WHERE billing_variant = '/billing'

),

jd_exp_billing_v1_metrics_source_table_000 AS (

  SELECT * 
  
  FROM jd_exp_billing_with_metrics

),

jd_exp_billing_v2_bounds_source_table_000 AS (

  SELECT * 
  
  FROM jd_exp_billing_with_metrics

),

jd_exp_billing_v2_bounds_from_000 AS (

  SELECT 
    session_date,
    billing_variant
  
  FROM jd_exp_billing_v2_bounds_source_table_000

),

jd_exp_billing_v2_bounds_filter_001 AS (

  SELECT * 
  
  FROM jd_exp_billing_v2_bounds_from_000
  
  WHERE billing_variant = '/billing-2'

),

jd_exp_billing_v2_bounds_projection_002 AS (

  SELECT 
    MIN(session_date) AS v2_first,
    MAX(session_date) AS v2_last
  
  FROM jd_exp_billing_v2_bounds_filter_001

),

jd_exp_billing_v1_bounds_projection_002 AS (

  SELECT 
    MIN(session_date) AS v1_first,
    MAX(session_date) AS v1_last
  
  FROM jd_exp_billing_v1_bounds_filter_001

),

jd_exp_overlap_window AS (

  SELECT 
    GREATEST(v1.v1_first, v2.v2_first) AS overlap_start,
    LEAST(v1.v1_last, v2.v2_last) AS overlap_end
  
  FROM jd_exp_billing_v1_bounds_projection_002 AS v1
  CROSS JOIN jd_exp_billing_v2_bounds_projection_002 AS v2

),

jd_exp_billing_v1_metrics_source_table_001 AS (

  SELECT * 
  
  FROM jd_exp_overlap_window

),

jd_exp_billing_v1_metrics_from_000 AS (

  SELECT 
    bm.billing_variant,
    ow.overlap_start,
    bm.ordered_flag,
    ow.overlap_end,
    bm.session_date
  
  FROM jd_exp_billing_v1_metrics_source_table_000 AS bm
  CROSS JOIN jd_exp_billing_v1_metrics_source_table_001 AS ow

),

jd_exp_billing_v1_metrics_filter_001 AS (

  SELECT * 
  
  FROM jd_exp_billing_v1_metrics_from_000
  
  WHERE billing_variant = '/billing' AND session_date >= overlap_start AND session_date <= overlap_end

),

jd_exp_billing_v1_metrics_projection_002 AS (

  SELECT 
    COUNT(*) AS baseline_sessions,
    SUM(ordered_flag) AS baseline_conversions
  
  FROM jd_exp_billing_v1_metrics_filter_001

),

jd_exp_billing_v2_metrics_source_table_000 AS (

  SELECT * 
  
  FROM jd_exp_billing_with_metrics

),

jd_exp_billing_v2_metrics_source_table_001 AS (

  SELECT * 
  
  FROM jd_exp_overlap_window

),

jd_exp_billing_v2_metrics_from_000 AS (

  SELECT 
    bm.billing_variant,
    bm.pageview_count,
    ow.overlap_start,
    ow.overlap_end,
    bm.ordered_flag,
    bm.session_date
  
  FROM jd_exp_billing_v2_metrics_source_table_000 AS bm
  CROSS JOIN jd_exp_billing_v2_metrics_source_table_001 AS ow

),

jd_exp_billing_v2_metrics_filter_001 AS (

  SELECT * 
  
  FROM jd_exp_billing_v2_metrics_from_000
  
  WHERE billing_variant = '/billing-2' AND session_date >= overlap_start AND session_date <= overlap_end

),

jd_exp_billing_v2_metrics_groupBy_002 AS (

  SELECT 
    'billing_page' AS experiment_family,
    overlap_start AS comparison_window_start,
    overlap_end AS comparison_window_end,
    '/billing-2' AS variant_key,
    '/billing' AS baseline_key,
    COUNT(*) AS sessions,
    SUM(ordered_flag) AS conversions,
    SUM(CASE
      WHEN pageview_count = 1
        THEN 1
      ELSE 0
    END) AS bounces
  
  FROM jd_exp_billing_v2_metrics_filter_001
  
  GROUP BY 
    overlap_start, overlap_end

),

jd_exp_billing_combined AS (

  SELECT 
    v2.experiment_family,
    v2.comparison_window_start,
    v2.comparison_window_end,
    v2.variant_key,
    v2.baseline_key,
    v2.sessions,
    v2.conversions,
    v2.bounces,
    COALESCE(v1.baseline_sessions, 0) AS baseline_sessions,
    COALESCE(v1.baseline_conversions, 0) AS baseline_conversions
  
  FROM jd_exp_billing_v2_metrics_groupBy_002 AS v2
  CROSS JOIN jd_exp_billing_v1_metrics_projection_002 AS v1

),

jd_exp_final AS (

  SELECT 
    CAST(experiment_family AS STRING) AS experiment_family,
    CAST(comparison_window_start AS DATE) AS comparison_window_start,
    CAST(comparison_window_end AS DATE) AS comparison_window_end,
    CAST(variant_key AS STRING) AS variant_key,
    CAST(baseline_key AS STRING) AS baseline_key,
    CAST(sessions AS BIGINT) AS sessions,
    CAST(conversions AS BIGINT) AS conversions,
    CAST(bounces AS BIGINT) AS bounces,
    CAST(CASE
      WHEN sessions > 0
        THEN CAST(conversions AS DOUBLE) / CAST(sessions AS DOUBLE)
      ELSE 0.0
    END AS DOUBLE) AS session_conversion_rate,
    CAST(CASE
      WHEN sessions > 0
        THEN CAST(bounces AS DOUBLE) / CAST(sessions AS DOUBLE)
      ELSE 0.0
    END AS DOUBLE) AS bounce_rate,
    CAST(CASE
      WHEN baseline_sessions > 0 AND sessions > 0
        THEN (
          (CAST(conversions AS DOUBLE) / CAST(sessions AS DOUBLE))
          - (CAST(baseline_conversions AS DOUBLE) / CAST(baseline_sessions AS DOUBLE))
        )
        / (CAST(baseline_conversions AS DOUBLE) / CAST(baseline_sessions AS DOUBLE))
      ELSE NULL
    END AS DOUBLE) AS relative_lift,
    CAST(CASE
      WHEN baseline_sessions > 0
      AND sessions > 0
      AND (CAST(conversions AS DOUBLE) / CAST(sessions AS DOUBLE)) > (CAST(baseline_conversions AS DOUBLE) / CAST(baseline_sessions AS DOUBLE))
        THEN 'positive'
      WHEN baseline_sessions > 0
      AND sessions > 0
      AND (CAST(conversions AS DOUBLE) / CAST(sessions AS DOUBLE)) < (CAST(baseline_conversions AS DOUBLE) / CAST(baseline_sessions AS DOUBLE))
        THEN 'negative'
      ELSE 'neutral'
    END AS STRING) AS finding_direction
  
  FROM jd_exp_billing_combined

)

SELECT *

FROM jd_exp_final
