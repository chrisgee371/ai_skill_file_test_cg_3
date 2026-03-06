# Iteration overview

This skill still uses five build iterations, but those iterations now land in **four smaller pipelines** instead of one giant pipeline.

## Iteration 1 - source intake and profiling

### Target pipeline
- `commerce_foundation`

Register the supplied Databricks source tables, expose the raw source layer, and confirm schema facts from support tables.

Outputs:
- `src__website_sessions`
- `src__website_pageviews`
- `src__orders`
- `src__order_items`
- `src__order_item_refunds`
- `src__products`

## Iteration 2 - canonical commerce graph

### Target pipeline
- `commerce_foundation`

Build the stable intermediate layer that later pipelines will consume through materialized handoff tables and `sources.yml`.

Outputs:
- `stg__website_sessions`
- `stg__website_pageviews`
- `stg__orders`
- `stg__order_items`
- `stg__order_item_refunds`
- `dim__products`
- `int__session_entry_page`
- `int__session_page_sequence`
- `int__session_order_bridge`
- `int__order_item_net_value`

## Iteration 3 - acquisition, journey, and experiments

### Target pipeline
- `journey_diagnostics`

Build top-of-funnel marts and leak diagnostics while respecting valid overlap windows for variants.
Consume `commerce_foundation` handoff outputs via `sources.yml` and `{{ source() }}`.

Outputs:
- `mart__traffic_source_day`
- `mart__landing_page_day`
- `mart__funnel_step_day`
- `mart__campaign_variant_day`
- `mart__checkout_variant_day`
- `diag__acquisition_leak`
- `diag__journey_leak`
- `diag__experiment_findings`

## Iteration 4 - products, basket, refunds, and launches

### Target pipeline
- `product_value_diagnostics`

Build post-purchase value diagnostics and launch-window analysis.
Consume `commerce_foundation` handoff outputs via `sources.yml` and `{{ source() }}`.

Outputs:
- `mart__product_day`
- `mart__primary_product_cohort`
- `mart__bundle_attachment`
- `mart__refund_rate_day`
- `diag__basket_leak`
- `diag__refund_leak`
- `diag__launch_leak`

## Iteration 5 - unified observatory and actions

### Target pipeline
- `observatory_reporting`

Build the final decision layer.
Consume diagnostic handoff outputs from `journey_diagnostics` and `product_value_diagnostics` via `sources.yml` and `{{ source() }}`.

Outputs:
- `obs__leak_registry`
- `obs__priority_actions`
- `obs__executive_scorecard`
- `obs__leak_explainers`
