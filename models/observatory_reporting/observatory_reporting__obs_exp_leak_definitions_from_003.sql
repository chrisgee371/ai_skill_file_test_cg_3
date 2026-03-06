{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH obs_exp_leak_definitions_from_003 AS (

  SELECT 
    'refund_leak' AS leak_family,
    'Revenue lost through product returns and refunds' AS definition,
    'High refund rates, quality complaints, expectation mismatch' AS symptoms,
    'Direct revenue loss, customer trust erosion, COGS loss' AS business_impact,
    'product' AS entity_types,
    'operations, product' AS owner_teams

)

SELECT *

FROM obs_exp_leak_definitions_from_003
