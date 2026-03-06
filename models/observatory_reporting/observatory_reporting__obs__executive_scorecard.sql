{{
  config({    
    "materialized": "table",
    "alias": "observatory_reporting__obs__executive_scorecard",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH obs_esc_refund_leak AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'product_value_diagnostics__diag__refund_leak') }}

),

obs_esc_refund_summary AS (

  SELECT 
    'refund_leak' AS leak_family,
    COUNT(*) AS total_entities,
    SUM(CASE
      WHEN recommended_action IS NOT NULL
        THEN 1
      ELSE 0
    END) AS entities_with_issues,
    AVG(severity_score) AS avg_severity,
    MAX(severity_score) AS max_severity,
    SUM(items_sold) AS total_volume,
    SUM(total_refund_amount) AS estimated_revenue_impact
  
  FROM obs_esc_refund_leak

),

obs_esc_basket_leak AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'product_value_diagnostics__diag__basket_leak') }}

),

obs_esc_basket_summary AS (

  SELECT 
    'basket_leak' AS leak_family,
    COUNT(*) AS total_entities,
    SUM(CASE
      WHEN recommended_action IS NOT NULL
        THEN 1
      ELSE 0
    END) AS entities_with_issues,
    AVG(severity_score) AS avg_severity,
    MAX(severity_score) AS max_severity,
    SUM(total_orders) AS total_volume,
    SUM(total_order_value - total_crosssell_value) AS estimated_revenue_impact
  
  FROM obs_esc_basket_leak

),

obs_esc_journey_leak AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'journey_diagnostics__diag__journey_leak') }}

),

obs_esc_journey_summary AS (

  SELECT 
    'journey_leak' AS leak_family,
    COUNT(*) AS total_entities,
    SUM(CASE
      WHEN recommended_action IS NOT NULL
        THEN 1
      ELSE 0
    END) AS entities_with_issues,
    AVG(severity_score) AS avg_severity,
    MAX(severity_score) AS max_severity,
    SUM(sessions_entering) AS total_volume,
    NULL AS estimated_revenue_impact
  
  FROM obs_esc_journey_leak

),

obs_esc_acquisition_leak AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'journey_diagnostics__diag__acquisition_leak') }}

),

obs_esc_launch_leak AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'product_value_diagnostics__diag__launch_leak') }}

),

obs_esc_launch_summary AS (

  SELECT 
    'launch_leak' AS leak_family,
    COUNT(*) AS total_entities,
    SUM(CASE
      WHEN recommended_action IS NOT NULL
        THEN 1
      ELSE 0
    END) AS entities_with_issues,
    AVG(severity_score) AS avg_severity,
    MAX(severity_score) AS max_severity,
    SUM(launch_items_sold) AS total_volume,
    SUM(launch_refund_amount) AS estimated_revenue_impact
  
  FROM obs_esc_launch_leak

),

obs_esc_acquisition_summary AS (

  SELECT 
    'acquisition_leak' AS leak_family,
    COUNT(*) AS total_entities,
    SUM(CASE
      WHEN recommended_action IS NOT NULL
        THEN 1
      ELSE 0
    END) AS entities_with_issues,
    AVG(severity_score) AS avg_severity,
    MAX(severity_score) AS max_severity,
    SUM(sessions) AS total_volume,
    SUM(revenue_per_session * sessions) AS estimated_revenue_impact
  
  FROM obs_esc_acquisition_leak

),

obs_esc_combined AS (

  SELECT * 
  
  FROM obs_esc_acquisition_summary
  
  UNION ALL
  
  SELECT * 
  
  FROM obs_esc_journey_summary
  
  UNION ALL
  
  SELECT * 
  
  FROM obs_esc_basket_summary
  
  UNION ALL
  
  SELECT * 
  
  FROM obs_esc_refund_summary
  
  UNION ALL
  
  SELECT * 
  
  FROM obs_esc_launch_summary

),

obs_esc_final AS (

  SELECT 
    CAST(leak_family AS STRING) AS leak_family,
    CAST(total_entities AS BIGINT) AS total_entities,
    CAST(entities_with_issues AS BIGINT) AS entities_with_issues,
    CAST(CASE
      WHEN total_entities > 0
        THEN (entities_with_issues * 1.0) / total_entities
      ELSE 0.0
    END AS DOUBLE) AS issue_rate,
    CAST(avg_severity AS DOUBLE) AS avg_severity,
    CAST(max_severity AS DOUBLE) AS max_severity,
    CAST(total_volume AS BIGINT) AS total_volume,
    CAST(estimated_revenue_impact AS DOUBLE) AS estimated_revenue_impact,
    CAST(CASE
      WHEN max_severity > 80.0
        THEN 'critical'
      WHEN max_severity > 60.0
        THEN 'high'
      WHEN max_severity > 40.0
        THEN 'medium'
      ELSE 'low'
    END AS STRING) AS urgency_level
  
  FROM obs_esc_combined

)

SELECT *

FROM obs_esc_final
