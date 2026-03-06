{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH obs_exp_leak_definitions_from_004 AS (

  SELECT 
    'launch_leak' AS leak_family,
    'Revenue lost from underperforming new product launches' AS definition,
    'Low launch sales, high early refunds, slow adoption' AS symptoms,
    'Failed product investment, market opportunity loss' AS business_impact,
    'product_launch' AS entity_types,
    'product, marketing' AS owner_teams

)

SELECT *

FROM obs_exp_leak_definitions_from_004
