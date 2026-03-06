{{
  config({    
    "materialized": "table",
    "alias": "obs__leak_registry",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH olr_refund AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__refund_leak') }}

),

olr_rfd_mapped_source_table_000 AS (

  SELECT * 
  
  FROM olr_refund

),

olr_launch AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__launch_leak') }}

),

olr_lch_mapped_source_table_000 AS (

  SELECT * 
  
  FROM olr_launch

),

olr_lch_mapped_from_000 AS (

  SELECT 
    dimension_value AS entity_key,
    refunds_first_30d,
    items_sold_first_30d,
    leak_severity,
    leak_dimension AS entity_type,
    product_launch_date AS period_start,
    estimated_launch_leak_usd,
    dimension_value,
    'launch_leak' AS leak_type,
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
    CONCAT(
      '{"items_sold_first_30d":', 
      CAST(items_sold_first_30d AS STRING), 
      ',"refunds_first_30d":', 
      CAST(refunds_first_30d AS STRING), 
      ',"estimated_launch_leak_usd":', 
      CAST(COALESCE(estimated_launch_leak_usd, 0) AS STRING), 
      '}') AS supporting_metrics,
    CASE
      WHEN items_sold_first_30d >= 50
        THEN 'high confidence'
      WHEN items_sold_first_30d >= 10
        THEN 'moderate confidence'
      ELSE 'low confidence - small sample'
    END AS confidence_note,
    CONCAT('Review launch quality for: ', dimension_value) AS recommended_action,
    '["diag__launch_leak"]' AS upstream_model_refs
  
  FROM olr_lch_mapped_source_table_000

),

olr_basket AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__basket_leak') }}

),

olr_bsk_mapped_source_table_000 AS (

  SELECT * 
  
  FROM olr_basket

),

olr_bsk_mapped_from_000 AS (

  SELECT 
    dimension_value AS entity_key,
    leak_severity,
    leak_dimension AS entity_type,
    cross_sell_rate_pct,
    estimated_leaked_cross_sell_revenue_usd,
    orders,
    dimension_value,
    CURRENT_DATE() AS period_start,
    'basket_leak' AS leak_type,
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
    CONCAT(
      '{"orders":', 
      CAST(orders AS STRING), 
      ',"cross_sell_rate_pct":', 
      CAST(COALESCE(cross_sell_rate_pct, 0) AS STRING), 
      ',"estimated_leaked_cross_sell_revenue_usd":', 
      CAST(COALESCE(estimated_leaked_cross_sell_revenue_usd, 0) AS STRING), 
      '}') AS supporting_metrics,
    CASE
      WHEN orders >= 50
        THEN 'high confidence'
      WHEN orders >= 10
        THEN 'moderate confidence'
      ELSE 'low confidence - small sample'
    END AS confidence_note,
    CONCAT('Improve cross-sell for: ', dimension_value) AS recommended_action,
    '["diag__basket_leak"]' AS upstream_model_refs
  
  FROM olr_bsk_mapped_source_table_000

),

olr_bsk_mapped_filter_001 AS (

  SELECT * 
  
  FROM olr_bsk_mapped_from_000
  
  WHERE leak_severity IN ('critical', 'warning', 'moderate')

),

olr_journey AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__journey_leak') }}

),

olr_jny_mapped_source_table_000 AS (

  SELECT * 
  
  FROM olr_journey

),

olr_lch_mapped_filter_001 AS (

  SELECT * 
  
  FROM olr_lch_mapped_from_000
  
  WHERE leak_severity IN ('critical', 'warning', 'moderate')

),

olr_lch_mapped_projection_002 AS (

  SELECT 
    period_start,
    entity_type,
    entity_key,
    'launch_leak' AS leak_type,
    severity_score,
    supporting_metrics,
    confidence_note,
    recommended_action,
    '["diag__launch_leak"]' AS upstream_model_refs
  
  FROM olr_lch_mapped_filter_001

),

olr_acquisition AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__acquisition_leak') }}

),

olr_acq_mapped_source_table_000 AS (

  SELECT * 
  
  FROM olr_acquisition

),

olr_acq_mapped_from_000 AS (

  SELECT 
    dimension_value AS entity_key,
    leak_severity,
    leak_dimension AS entity_type,
    sessions,
    leak_dimension,
    conversions,
    estimated_leaked_revenue_usd,
    CURRENT_DATE() AS period_start,
    'acquisition_leak' AS leak_type,
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
    CONCAT(
      '{"sessions":', 
      CAST(sessions AS STRING), 
      ',"conversions":', 
      CAST(conversions AS STRING), 
      ',"estimated_leaked_revenue_usd":', 
      CAST(COALESCE(estimated_leaked_revenue_usd, 0) AS STRING), 
      '}') AS supporting_metrics,
    CASE
      WHEN sessions >= 100
        THEN 'high confidence'
      WHEN sessions >= 20
        THEN 'moderate confidence'
      ELSE 'low confidence - small sample'
    END AS confidence_note,
    CONCAT('Optimize ', leak_dimension, ' performance') AS recommended_action,
    '["diag__acquisition_leak"]' AS upstream_model_refs
  
  FROM olr_acq_mapped_source_table_000

),

olr_acq_mapped_filter_001 AS (

  SELECT * 
  
  FROM olr_acq_mapped_from_000
  
  WHERE leak_severity IN ('critical', 'warning', 'moderate')

),

olr_acq_mapped_projection_002 AS (

  SELECT 
    period_start,
    entity_type,
    entity_key,
    'acquisition_leak' AS leak_type,
    severity_score,
    supporting_metrics,
    confidence_note,
    recommended_action,
    '["diag__acquisition_leak"]' AS upstream_model_refs
  
  FROM olr_acq_mapped_filter_001

),

olr_rfd_mapped_from_000 AS (

  SELECT 
    dimension_value AS entity_key,
    leak_severity,
    estimated_excess_refund_usd,
    leak_dimension AS entity_type,
    items_refunded,
    items_sold,
    dimension_value,
    CURRENT_DATE() AS period_start,
    'refund_leak' AS leak_type,
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
    CONCAT(
      '{"items_sold":', 
      CAST(items_sold AS STRING), 
      ',"items_refunded":', 
      CAST(items_refunded AS STRING), 
      ',"estimated_excess_refund_usd":', 
      CAST(COALESCE(estimated_excess_refund_usd, 0) AS STRING), 
      '}') AS supporting_metrics,
    CASE
      WHEN items_sold >= 50
        THEN 'high confidence'
      WHEN items_sold >= 10
        THEN 'moderate confidence'
      ELSE 'low confidence - small sample'
    END AS confidence_note,
    CONCAT('Reduce refunds for: ', dimension_value) AS recommended_action,
    '["diag__refund_leak"]' AS upstream_model_refs
  
  FROM olr_rfd_mapped_source_table_000

),

olr_rfd_mapped_filter_001 AS (

  SELECT * 
  
  FROM olr_rfd_mapped_from_000
  
  WHERE leak_severity IN ('critical', 'warning', 'moderate')

),

olr_rfd_mapped_projection_002 AS (

  SELECT 
    period_start,
    entity_type,
    entity_key,
    'refund_leak' AS leak_type,
    severity_score,
    supporting_metrics,
    confidence_note,
    recommended_action,
    '["diag__refund_leak"]' AS upstream_model_refs
  
  FROM olr_rfd_mapped_filter_001

),

olr_jny_mapped_from_000 AS (

  SELECT 
    dimension_value AS entity_key,
    leak_severity,
    leak_dimension AS entity_type,
    estimated_leaked_revenue_usd,
    sessions_dropped,
    dimension_value,
    sessions_entering,
    CURRENT_DATE() AS period_start,
    'journey_leak' AS leak_type,
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
    CONCAT(
      '{"sessions_entering":', 
      CAST(sessions_entering AS STRING), 
      ',"sessions_dropped":', 
      CAST(sessions_dropped AS STRING), 
      ',"estimated_leaked_revenue_usd":', 
      CAST(COALESCE(estimated_leaked_revenue_usd, 0) AS STRING), 
      '}') AS supporting_metrics,
    CASE
      WHEN sessions_entering >= 100
        THEN 'high confidence'
      WHEN sessions_entering >= 20
        THEN 'moderate confidence'
      ELSE 'low confidence - small sample'
    END AS confidence_note,
    CONCAT('Improve funnel step: ', dimension_value) AS recommended_action,
    '["diag__journey_leak"]' AS upstream_model_refs
  
  FROM olr_jny_mapped_source_table_000

),

olr_jny_mapped_filter_001 AS (

  SELECT * 
  
  FROM olr_jny_mapped_from_000
  
  WHERE leak_severity IN ('critical', 'warning', 'moderate')

),

olr_jny_mapped_projection_002 AS (

  SELECT 
    period_start,
    entity_type,
    entity_key,
    'journey_leak' AS leak_type,
    severity_score,
    supporting_metrics,
    confidence_note,
    recommended_action,
    '["diag__journey_leak"]' AS upstream_model_refs
  
  FROM olr_jny_mapped_filter_001

),

olr_bsk_mapped_projection_002 AS (

  SELECT 
    period_start,
    entity_type,
    entity_key,
    'basket_leak' AS leak_type,
    severity_score,
    supporting_metrics,
    confidence_note,
    recommended_action,
    '["diag__basket_leak"]' AS upstream_model_refs
  
  FROM olr_bsk_mapped_filter_001

),

olr_combined AS (

  SELECT * 
  
  FROM olr_acq_mapped_projection_002 AS olr_acq_mapped
  
  UNION ALL
  
  SELECT * 
  
  FROM olr_jny_mapped_projection_002 AS olr_jny_mapped
  
  UNION ALL
  
  SELECT * 
  
  FROM olr_bsk_mapped_projection_002 AS olr_bsk_mapped
  
  UNION ALL
  
  SELECT * 
  
  FROM olr_rfd_mapped_projection_002 AS olr_rfd_mapped
  
  UNION ALL
  
  SELECT * 
  
  FROM olr_lch_mapped_projection_002 AS olr_lch_mapped

),

olr_final AS (

  SELECT 
    CAST(period_start AS DATE) AS period_start,
    CAST(entity_type AS STRING) AS entity_type,
    CAST(entity_key AS STRING) AS entity_key,
    CAST(leak_type AS STRING) AS leak_type,
    CAST(severity_score AS DOUBLE) AS severity_score,
    CAST(supporting_metrics AS STRING) AS supporting_metrics,
    CAST(confidence_note AS STRING) AS confidence_note,
    CAST(recommended_action AS STRING) AS recommended_action,
    CAST(upstream_model_refs AS STRING) AS upstream_model_refs
  
  FROM olr_combined

)

SELECT *

FROM olr_final
