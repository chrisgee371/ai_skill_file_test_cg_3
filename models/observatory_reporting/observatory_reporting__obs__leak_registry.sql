{{
  config({    
    "materialized": "table",
    "alias": "observatory_reporting__obs__leak_registry",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH obs_reg_acquisition_leak AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'journey_diagnostics__diag__acquisition_leak') }}

),

obs_reg_launch_leak AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'product_value_diagnostics__diag__launch_leak') }}

),

obs_reg_basket_leak AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'product_value_diagnostics__diag__basket_leak') }}

),

obs_reg_basket_normalized AS (

  SELECT 
    analysis_date,
    'basket_leak' AS leak_family,
    entity_type,
    entity_key,
    total_orders AS volume_metric,
    crosssell_rate AS primary_rate_metric,
    total_crosssell_value AS value_metric,
    severity_score,
    recommended_action
  
  FROM obs_reg_basket_leak

),

obs_reg_journey_leak AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'journey_diagnostics__diag__journey_leak') }}

),

obs_reg_journey_normalized AS (

  SELECT 
    analysis_date,
    'journey_leak' AS leak_family,
    entity_type,
    entity_key,
    sessions_entering AS volume_metric,
    CASE
      WHEN sessions_entering > 0
        THEN CAST(sessions_dropped AS DOUBLE) / CAST(sessions_entering AS DOUBLE)
      ELSE 0.0
    END AS primary_rate_metric,
    NULL AS value_metric,
    severity_score,
    recommended_action
  
  FROM obs_reg_journey_leak

),

obs_reg_launch_normalized AS (

  SELECT 
    analysis_date,
    'launch_leak' AS leak_family,
    entity_type,
    entity_key,
    launch_items_sold AS volume_metric,
    launch_refund_rate AS primary_rate_metric,
    launch_net_revenue AS value_metric,
    severity_score,
    recommended_action
  
  FROM obs_reg_launch_leak

),

obs_reg_acquisition_normalized AS (

  SELECT 
    analysis_date,
    'acquisition_leak' AS leak_family,
    entity_type,
    entity_key,
    sessions AS volume_metric,
    bounce_rate AS primary_rate_metric,
    revenue_per_session AS value_metric,
    severity_score,
    recommended_action
  
  FROM obs_reg_acquisition_leak

),

obs_reg_refund_leak AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'product_value_diagnostics__diag__refund_leak') }}

),

obs_reg_refund_normalized AS (

  SELECT 
    analysis_date,
    'refund_leak' AS leak_family,
    entity_type,
    entity_key,
    items_sold AS volume_metric,
    refund_rate AS primary_rate_metric,
    total_refund_amount AS value_metric,
    severity_score,
    recommended_action
  
  FROM obs_reg_refund_leak

),

obs_reg_combined AS (

  SELECT * 
  
  FROM obs_reg_acquisition_normalized
  
  UNION ALL
  
  SELECT * 
  
  FROM obs_reg_journey_normalized
  
  UNION ALL
  
  SELECT * 
  
  FROM obs_reg_basket_normalized
  
  UNION ALL
  
  SELECT * 
  
  FROM obs_reg_refund_normalized
  
  UNION ALL
  
  SELECT * 
  
  FROM obs_reg_launch_normalized

),

obs_reg_final AS (

  SELECT 
    CAST(analysis_date AS DATE) AS analysis_date,
    CAST(leak_family AS STRING) AS leak_family,
    CAST(entity_type AS STRING) AS entity_type,
    CAST(entity_key AS STRING) AS entity_key,
    CAST(volume_metric AS BIGINT) AS volume_metric,
    CAST(primary_rate_metric AS DOUBLE) AS primary_rate_metric,
    CAST(value_metric AS DOUBLE) AS value_metric,
    CAST(severity_score AS DOUBLE) AS severity_score,
    CAST(recommended_action AS STRING) AS recommended_action
  
  FROM obs_reg_combined

)

SELECT *

FROM obs_reg_final
