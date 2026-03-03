# Iteration overview

This project is intentionally delivered in five stages. That is large enough to keep each pass safe for an agent, but not so fragmented that progress becomes ceremonial.

## Iteration 1 - source intake and profiling

Build only the source-entry layer and verify that the pipeline matches the supplied Databricks source tables exactly.

Outputs:
- src__website_sessions
- src__website_pageviews
- src__orders
- src__order_items
- src__order_item_refunds
- src__products

## Iteration 2 - canonical commerce graph

Build the clean internal layer that later stages can trust and reference with `{{ ref('model_name') }}`.

Outputs:
- stg__website_sessions
- stg__website_pageviews
- stg__orders
- stg__order_items
- stg__order_item_refunds
- dim__products
- int__session_entry_page
- int__session_page_sequence
- int__session_order_bridge
- int__order_item_net_value

## Iteration 3 - acquisition, journey, and experiments

Build top-of-funnel marts and leak diagnostics while respecting valid overlap windows for variants.

Outputs:
- mart__traffic_source_day
- mart__landing_page_day
- mart__funnel_step_day
- mart__campaign_variant_day
- mart__checkout_variant_day
- diag__acquisition_leak
- diag__journey_leak
- diag__experiment_findings

## Iteration 4 - products, basket, refunds, and launches

Build post-purchase value diagnostics and launch-window analysis.

Outputs:
- mart__product_day
- mart__primary_product_cohort
- mart__bundle_attachment
- mart__refund_rate_day
- diag__basket_leak
- diag__refund_leak
- diag__launch_leak

## Iteration 5 - unified observatory and actions

Build the final decision layer.

Outputs:
- obs__leak_registry
- obs__priority_actions
- obs__executive_scorecard
- obs__leak_explainers
