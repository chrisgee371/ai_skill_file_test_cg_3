# Iteration 04 - Product, basket, refund, and launch diagnostics

## Target pipeline
- `product_value_diagnostics`

## Objective

Diagnose how value leaks after the shopper gets close to or completes purchase.

## Read these resources first

- `skills/toy-store-revenue-leak-observatory/docs/pipeline-topology.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-workflow.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-sql-rules.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-naming.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-server-behavior.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-model-patterns.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-sources-yml.md`
- `chris_demos.demos.chronology_summary`
- `chris_demos.demos.metric_catalog`
- `skills/toy-store-revenue-leak-observatory/docs/known-quirks.md`

## Inputs

Consume `commerce_foundation` handoff outputs through `sources.yml` and `{{ source() }}`.

- Do not use cross-pipeline `{{ ref() }}` back into `commerce_foundation`.
- Inside `product_value_diagnostics` itself, same-pipeline `ref()` is allowed only with matching `input_ports` and `>>` connections.

## What to build in this iteration

- `mart__product_day`
- `mart__primary_product_cohort`
- `mart__bundle_attachment`
- `mart__refund_rate_day`
- `diag__basket_leak`
- `diag__refund_leak`
- `diag__launch_leak`

## Diagnostic intent

### Basket leak
Find products or periods where:
- average order value underperforms
- items per order are weak
- bundle attachment is poor

### Refund leak
Find products or cohorts where refunds erase too much apparent growth.

### Launch leak
Identify launch windows where a new product creates demand but damages net-value quality.

## Explicit constraints

- Keep product logic appropriate for a four-product catalog.
- Use refund-aware net value, not gross revenue alone.
- Do not create launch windows that begin before product launch timestamps.
- Materialize the diagnostic handoff outputs needed by `observatory_reporting` and create their `sources.yml` entries.

## Datatype requirements (do not skip)

The following output columns must be `Long` (Spark) / `BIGINT` (Databricks SQL). Do not rely on implicit inference.
Cast them explicitly using `CAST(<expr> AS BIGINT)`.

- `diag__basket_leak.orders`
- `diag__basket_leak.orders_with_cross_sell`
- `diag__refund_leak.items_sold`
- `diag__refund_leak.items_refunded`
- `diag__launch_leak.items_sold_first_30d`
- `diag__launch_leak.refunds_first_30d`
- `diag__launch_leak.items_sold_lifetime`
- `diag__launch_leak.refunds_lifetime`

## Completion standard

This iteration is complete only when the post-purchase diagnostics can explain how gross success and net success diverge, and the downstream handoff outputs are ready for `observatory_reporting`.

## Prophecy implementation reminder

- The model names listed above are logical shortnames.
- Physical model names and filenames must follow `<pipeline_name>__<model_shortname>`.
- For this iteration, the physical pipeline name is `product_value_diagnostics`.
- Cross-pipeline inputs from `commerce_foundation` must arrive through source entries, not direct canvas or `ref()` reuse.
- Keep CTE names globally unique across the project.
- After editing files, call `update_files()`, inspect the surviving files, and continue from the post-compile state.
