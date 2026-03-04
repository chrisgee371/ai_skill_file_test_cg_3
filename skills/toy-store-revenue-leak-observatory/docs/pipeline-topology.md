# Pipeline topology

This skill uses **four smaller Prophecy pipelines** instead of one giant pipeline.

## Why this topology exists

Prophecy server behavior is more stable when:
- linear logic chains are kept short
- cross-domain joins are not forced into one giant canvas
- reusable handoff points are materialized as Databricks tables or views
- downstream pipelines consume those handoffs via `sources.yml`

This topology is therefore part of the skill contract, not just a preference.

## Pipeline 1 - `commerce_foundation`

### Purpose
Create the reusable core commerce graph from the raw Maven source tables.

### Owns stages
- Iteration 1
- Iteration 2

### Owns models
- `src__website_sessions`
- `src__website_pageviews`
- `src__orders`
- `src__order_items`
- `src__order_item_refunds`
- `src__products`
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

### Handoff outputs
Materialize these for downstream reuse:
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

### Downstream consumers
- `journey_diagnostics`
- `product_value_diagnostics`

## Pipeline 2 - `journey_diagnostics`

### Purpose
Build traffic, landing-page, funnel, and experiment diagnostics.

### Owns stages
- Iteration 3

### Inputs
Consume the materialized handoff outputs from `commerce_foundation` via `sources.yml` and `{{ source() }}`.

### Owns models
- `mart__traffic_source_day`
- `mart__landing_page_day`
- `mart__funnel_step_day`
- `mart__campaign_variant_day`
- `mart__checkout_variant_day`
- `diag__acquisition_leak`
- `diag__journey_leak`
- `diag__experiment_findings`

### Handoff outputs
Materialize these for downstream reuse:
- `diag__acquisition_leak`
- `diag__journey_leak`
- `diag__experiment_findings`

### Downstream consumer
- `observatory_reporting`

## Pipeline 3 - `product_value_diagnostics`

### Purpose
Build product, basket, refund, and launch diagnostics.

### Owns stages
- Iteration 4

### Inputs
Consume the materialized handoff outputs from `commerce_foundation` via `sources.yml` and `{{ source() }}`.

### Owns models
- `mart__product_day`
- `mart__primary_product_cohort`
- `mart__bundle_attachment`
- `mart__refund_rate_day`
- `diag__basket_leak`
- `diag__refund_leak`
- `diag__launch_leak`

### Handoff outputs
Materialize these for downstream reuse:
- `diag__basket_leak`
- `diag__refund_leak`
- `diag__launch_leak`

### Downstream consumer
- `observatory_reporting`

## Pipeline 4 - `observatory_reporting`

### Purpose
Unify the diagnostic outputs into the final decision layer.

### Owns stages
- Iteration 5

### Inputs
Consume the materialized diagnostic handoff outputs from `journey_diagnostics` and `product_value_diagnostics` via `sources.yml` and `{{ source() }}`.

### Owns models
- `obs__leak_registry`
- `obs__priority_actions`
- `obs__executive_scorecard`
- `obs__leak_explainers`

## Mandatory handoff rule

Across pipeline boundaries:
- materialize the upstream model as a table or view
- create or update a `sources.yml` entry for it
- consume it downstream using `{{ source() }}`

Do **not** use cross-pipeline `ref()`.

## Naming rule

Every physical model name must follow:
- `<pipeline_name>__<model_shortname>`

Examples:
- `commerce_foundation__int__session_order_bridge`
- `journey_diagnostics__diag__acquisition_leak`
- `product_value_diagnostics__diag__refund_leak`
- `observatory_reporting__obs__leak_registry`
