# Iteration 03 - Acquisition, journey, and experiment diagnostics

## Target pipeline
- `journey_diagnostics`

## Objective

Diagnose where top-of-funnel and checkout performance is leaking value, while respecting chronology and valid comparison windows.

## Read these resources first

- `skills/toy-store-revenue-leak-observatory/docs/pipeline-topology.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-workflow.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-sql-rules.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-naming.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-server-behavior.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-model-patterns.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-sources-yml.md`
- `chris_demos.demos.page_catalog`
- `chris_demos.demos.traffic_catalog`
- `chris_demos.demos.chronology_summary`
- `skills/toy-store-revenue-leak-observatory/docs/known-quirks.md`
- `chris_demos.demos.metric_catalog`
- `chris_demos.demos.model_output_schemas`

## Inputs

Consume `commerce_foundation` handoff outputs through `sources.yml` and `{{ source() }}`.

- Do not use cross-pipeline `{{ ref() }}` back into `commerce_foundation`.
- Inside `journey_diagnostics` itself, same-pipeline `ref()` is allowed only with matching `input_ports` and `>>` connections.


## SQL pattern library preflight

Before you author any non-trivial SQL in this iteration:

- read `chris_demos.demos.databricks_sql_pattern_index` or `skills/toy-store-revenue-leak-observatory/support_files/databricks_sql_pattern_index.json`
- shortlist relevant pattern families and open the referenced raw pack files under `skills/toy-store-revenue-leak-observatory/support_files/databricks_sql_pattern_packs/`
- read `chris_demos.demos.databricks_sql_pattern_registry` and `chris_demos.demos.databricks_sql_pattern_composition_rules`, or their JSON fallbacks under `support_files/`
- prefer entries marked `preferred` or `allowed`
- do not emit entries marked `suitable_for_model_production = false`, `negative_control = true`, or `prophecy_skill_status = 'forbidden'`

Use the library for structure guidance, but keep the current iteration contract and datatype requirements as the final source of truth.

## What to build in this iteration

- `mart__traffic_source_day`
- `mart__landing_page_day`
- `mart__funnel_step_day`
- `mart__campaign_variant_day`
- `mart__checkout_variant_day`
- `diag__acquisition_leak`
- `diag__journey_leak`
- `diag__experiment_findings`

## Diagnostic intent

### Acquisition leak
Find channels, campaigns, or entry pages that drive sessions but underperform on:
- conversion
- bounce rate
- revenue per session
- net revenue per session

### Journey leak
Find page-path or funnel stages where the business is losing too many users before order completion.

### Experiment findings
Compare landing-page, ad-content, and billing-page variants only inside valid overlap windows.

## Explicit constraints

- Do not compare variants across the full history if the variants did not coexist.
- Treat `/billing` and `/billing-2` as a checkout variant family.
- Preserve `NULL` UTM traffic as a legitimate class.
- Make severity scores bounded and explainable.
- Materialize the diagnostic handoff outputs needed by `observatory_reporting` and create their `sources.yml` entries.

## Datatype requirements (do not skip)

The authoritative contract for this iteration lives in `chris_demos.demos.model_output_schemas`.
All named output fields must match that contract.

The following output columns are especially error-prone and must be `Long` (Spark) / `BIGINT` (Databricks SQL). Do not rely on implicit inference.
Cast them explicitly using `CAST(<expr> AS BIGINT)`.

- `diag__acquisition_leak.sessions`
- `diag__acquisition_leak.conversions`
- `diag__journey_leak.sessions_entering`
- `diag__journey_leak.sessions_continuing`
- `diag__journey_leak.sessions_dropped`
- `diag__experiment_findings.sessions`
- `diag__experiment_findings.conversions`
- `diag__experiment_findings.bounces`

## Completion standard

This iteration is complete only when the diagnostic layer can point to concrete top-of-funnel and journey leaks without breaking chronology, and the downstream handoff outputs are ready for `observatory_reporting`.

## Prophecy implementation reminder

- The model names listed above are logical shortnames.
- Physical model names and filenames must follow `<pipeline_name>__<model_shortname>`.
- For this iteration, the physical pipeline name is `journey_diagnostics`.
- Cross-pipeline inputs from `commerce_foundation` must arrive through source entries, not direct canvas or `ref()` reuse.
- Keep CTE names globally unique across the project.
- After editing files, call `update_files()`, inspect the surviving files, and continue from the post-compile state.
