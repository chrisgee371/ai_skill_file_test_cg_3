{% macro calculate_safe_percentage(numerator, denominator) %}
ROUND(
  CASE
    WHEN {{denominator}} IS NULL OR {{denominator}} = 0
      THEN 0
    ELSE (CAST({{numerator}} AS DOUBLE) / CAST({{denominator}} AS DOUBLE)) * 100
  END, 
  2)
{% endmacro %}

 