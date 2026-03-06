{{
  config({    
    "materialized": "table",
    "alias": "obs__priority_actions",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH opa_acquisition AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__acquisition_leak') }}

),

opa_acq_actions_source_table_000 AS (

  SELECT * 
  
  FROM opa_acquisition

),

opa_acq_actions_from_000 AS (

  SELECT 
    dimension_value AS entity_key,
    leak_severity,
    leak_dimension AS entity_type,
    leak_dimension,
    estimated_leaked_revenue_usd,
    dimension_value,
    CURRENT_DATE() AS period_start,
    'acquisition_leak' AS leak_type,
    CONCAT('Optimize ', leak_dimension, ': ', dimension_value) AS recommended_action,
    CASE
      WHEN leak_severity = 'critical'
        THEN 90.0
      WHEN leak_severity = 'warning'
        THEN 70.0
      WHEN leak_severity = 'moderate'
        THEN 40.0
      WHEN leak_severity = 'on_target'
        THEN 20.0
      ELSE 0.0
    END AS severity_score,
    COALESCE(estimated_leaked_revenue_usd, 0) AS expected_leak_reduction
  
  FROM opa_acq_actions_source_table_000

),

opa_acq_actions_filter_001 AS (

  SELECT * 
  
  FROM opa_acq_actions_from_000
  
  WHERE leak_severity IN ('critical', 'warning')

),

opa_acq_actions_projection_002 AS (

  SELECT 
    period_start,
    entity_type,
    entity_key,
    'acquisition_leak' AS leak_type,
    recommended_action,
    severity_score,
    expected_leak_reduction
  
  FROM opa_acq_actions_filter_001

),

opa_launch AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__launch_leak') }}

),

opa_lch_actions_source_table_000 AS (

  SELECT * 
  
  FROM opa_launch

),

opa_refund AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__refund_leak') }}

),

opa_rfd_actions_source_table_000 AS (

  SELECT * 
  
  FROM opa_refund

),

opa_basket AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__basket_leak') }}

),

opa_bsk_actions_source_table_000 AS (

  SELECT * 
  
  FROM opa_basket

),

opa_bsk_actions_from_000 AS (

  SELECT 
    dimension_value AS entity_key,
    leak_severity,
    leak_dimension AS entity_type,
    estimated_leaked_cross_sell_revenue_usd,
    dimension_value,
    CURRENT_DATE() AS period_start,
    'basket_leak' AS leak_type,
    CONCAT('Improve cross-sell for: ', dimension_value) AS recommended_action,
    CASE
      WHEN leak_severity = 'critical'
        THEN 90.0
      WHEN leak_severity = 'warning'
        THEN 70.0
      WHEN leak_severity = 'moderate'
        THEN 40.0
      WHEN leak_severity = 'on_target'
        THEN 20.0
      ELSE 0.0
    END AS severity_score,
    COALESCE(estimated_leaked_cross_sell_revenue_usd, 0) AS expected_leak_reduction
  
  FROM opa_bsk_actions_source_table_000

),

opa_bsk_actions_filter_001 AS (

  SELECT * 
  
  FROM opa_bsk_actions_from_000
  
  WHERE leak_severity IN ('critical', 'warning')

),

opa_bsk_actions_projection_002 AS (

  SELECT 
    period_start,
    entity_type,
    entity_key,
    'basket_leak' AS leak_type,
    recommended_action,
    severity_score,
    expected_leak_reduction
  
  FROM opa_bsk_actions_filter_001

),

opa_rfd_actions_from_000 AS (

  SELECT 
    dimension_value AS entity_key,
    leak_severity,
    estimated_excess_refund_usd,
    leak_dimension AS entity_type,
    dimension_value,
    CURRENT_DATE() AS period_start,
    'refund_leak' AS leak_type,
    CONCAT('Reduce refunds for: ', dimension_value) AS recommended_action,
    CASE
      WHEN leak_severity = 'critical'
        THEN 90.0
      WHEN leak_severity = 'warning'
        THEN 70.0
      WHEN leak_severity = 'moderate'
        THEN 40.0
      WHEN leak_severity = 'on_target'
        THEN 20.0
      ELSE 0.0
    END AS severity_score,
    COALESCE(estimated_excess_refund_usd, 0) AS expected_leak_reduction
  
  FROM opa_rfd_actions_source_table_000

),

opa_rfd_actions_filter_001 AS (

  SELECT * 
  
  FROM opa_rfd_actions_from_000
  
  WHERE leak_severity IN ('critical', 'warning')

),

opa_rfd_actions_projection_002 AS (

  SELECT 
    period_start,
    entity_type,
    entity_key,
    'refund_leak' AS leak_type,
    recommended_action,
    severity_score,
    expected_leak_reduction
  
  FROM opa_rfd_actions_filter_001

),

opa_journey AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__journey_leak') }}

),

opa_jny_actions_source_table_000 AS (

  SELECT * 
  
  FROM opa_journey

),

opa_jny_actions_from_000 AS (

  SELECT 
    dimension_value AS entity_key,
    leak_severity,
    leak_dimension AS entity_type,
    estimated_leaked_revenue_usd,
    dimension_value,
    CURRENT_DATE() AS period_start,
    'journey_leak' AS leak_type,
    CONCAT('Improve funnel step: ', dimension_value) AS recommended_action,
    CASE
      WHEN leak_severity = 'critical'
        THEN 90.0
      WHEN leak_severity = 'warning'
        THEN 70.0
      WHEN leak_severity = 'moderate'
        THEN 40.0
      WHEN leak_severity = 'on_target'
        THEN 20.0
      ELSE 0.0
    END AS severity_score,
    COALESCE(estimated_leaked_revenue_usd, 0) AS expected_leak_reduction
  
  FROM opa_jny_actions_source_table_000

),

opa_jny_actions_filter_001 AS (

  SELECT * 
  
  FROM opa_jny_actions_from_000
  
  WHERE leak_severity IN ('critical', 'warning')

),

opa_jny_actions_projection_002 AS (

  SELECT 
    period_start,
    entity_type,
    entity_key,
    'journey_leak' AS leak_type,
    recommended_action,
    severity_score,
    expected_leak_reduction
  
  FROM opa_jny_actions_filter_001

),

opa_lch_actions_from_000 AS (

  SELECT 
    dimension_value AS entity_key,
    leak_severity,
    leak_dimension AS entity_type,
    product_launch_date AS period_start,
    estimated_launch_leak_usd,
    dimension_value,
    'launch_leak' AS leak_type,
    CONCAT('Review launch quality for: ', dimension_value) AS recommended_action,
    CASE
      WHEN leak_severity = 'critical'
        THEN 90.0
      WHEN leak_severity = 'warning'
        THEN 70.0
      WHEN leak_severity = 'moderate'
        THEN 40.0
      WHEN leak_severity = 'on_target'
        THEN 20.0
      ELSE 0.0
    END AS severity_score,
    COALESCE(estimated_launch_leak_usd, 0) AS expected_leak_reduction
  
  FROM opa_lch_actions_source_table_000

),

opa_lch_actions_filter_001 AS (

  SELECT * 
  
  FROM opa_lch_actions_from_000
  
  WHERE leak_severity IN ('critical', 'warning')

),

opa_lch_actions_projection_002 AS (

  SELECT 
    period_start,
    entity_type,
    entity_key,
    'launch_leak' AS leak_type,
    recommended_action,
    severity_score,
    expected_leak_reduction
  
  FROM opa_lch_actions_filter_001

),

opa_combined AS (

  SELECT * 
  
  FROM opa_acq_actions_projection_002 AS opa_acq_actions
  
  UNION ALL
  
  SELECT * 
  
  FROM opa_jny_actions_projection_002 AS opa_jny_actions
  
  UNION ALL
  
  SELECT * 
  
  FROM opa_bsk_actions_projection_002 AS opa_bsk_actions
  
  UNION ALL
  
  SELECT * 
  
  FROM opa_rfd_actions_projection_002 AS opa_rfd_actions
  
  UNION ALL
  
  SELECT * 
  
  FROM opa_lch_actions_projection_002 AS opa_lch_actions

),

opa_ranked AS (

  SELECT 
    ROW_NUMBER() OVER (ORDER BY severity_score DESC, expected_leak_reduction DESC) AS priority_rank,
    period_start,
    entity_type,
    entity_key,
    leak_type,
    recommended_action,
    expected_leak_reduction,
    severity_score
  
  FROM opa_combined

),

opa_final AS (

  SELECT 
    CAST(priority_rank AS BIGINT) AS priority_rank,
    CAST(period_start AS DATE) AS period_start,
    CAST(entity_type AS STRING) AS entity_type,
    CAST(entity_key AS STRING) AS entity_key,
    CAST(leak_type AS STRING) AS leak_type,
    CAST(recommended_action AS STRING) AS recommended_action,
    CAST(expected_leak_reduction AS DOUBLE) AS expected_leak_reduction,
    CAST(severity_score AS DOUBLE) AS severity_score
  
  FROM opa_ranked

)

SELECT *

FROM opa_final
