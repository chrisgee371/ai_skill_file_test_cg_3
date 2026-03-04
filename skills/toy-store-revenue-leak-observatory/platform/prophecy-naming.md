# Prophecy naming

## Physical model naming pattern

Use:

`<pipeline_name>__<model_shortname>`

Examples:
- `commerce_foundation__src__website_sessions`
- `commerce_foundation__int__session_order_bridge`
- `journey_diagnostics__diag__acquisition_leak`
- `product_value_diagnostics__diag__refund_leak`
- `observatory_reporting__obs__leak_registry`

## Approved pipeline names for this skill

Use these pipeline names exactly:
- `commerce_foundation`
- `journey_diagnostics`
- `product_value_diagnostics`
- `observatory_reporting`

Pipeline names must be lowercase and use only alphanumeric characters plus underscore.

## Alignment rule

The following should all match the same physical name:
- SQL file name
- model name / modelName
- schema.yml model entry
- `ref()` target when `ref()` is the chosen same-pipeline dependency pattern

## CTE prefix rule

Derive CTE prefixes from the shortname to reduce collision risk.
