# Iteration 05 - Observatory and action layer

## Target pipeline
- `observatory_reporting`

## Objective

Turn the leak diagnostics into a unified decision layer that can be consumed by downstream users without reading every mart individually.

## Read these resources first

- `skills/toy-store-revenue-leak-observatory/docs/pipeline-topology.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-workflow.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-sql-rules.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-naming.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-server-behavior.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-model-patterns.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-sources-yml.md`
- `chris_demos.demos.model_registry`
- `chris_demos.demos.metric_catalog`
- `chris_demos.demos.acceptance_tests`
- `chris_demos.demos.model_output_schemas`

## Inputs

Consume the diagnostic handoff outputs from `journey_diagnostics` and `product_value_diagnostics` through `sources.yml` and `{{ source() }}`.

- Do not use cross-pipeline `{{ ref() }}` back into the diagnostic pipelines.
- Inside `observatory_reporting` itself, same-pipeline `ref()` is allowed only with matching `input_ports` and `>>` connections.

## What to build in this iteration

- `obs__leak_registry`
- `obs__priority_actions`
- `obs__executive_scorecard`
- `obs__leak_explainers`

## Design intent

### obs__leak_registry
One row per period, entity, and leak family, with:
- severity_score
- supporting_metrics
- confidence_note
- recommended_action
- upstream_model_refs

### obs__priority_actions
Rank actions by severity and likely value recovery.

### obs__executive_scorecard
Summarize the health of the business in a compact, decision-ready form.

### obs__leak_explainers
Provide human-readable justification for each finding.

## Explicit constraints

- Do not output opaque scores without support.
- Do not leave findings unranked.
- Do not hide uncertainty; state it.
- Preserve traceability to the supporting upstream models.


## Datatype requirements

Use `chris_demos.demos.model_output_schemas` as the source of truth for every output field in this iteration.

At minimum, ensure these fields are explicitly typed in the final projection:

- `obs__leak_registry.period_start` -> `DATE`
- `obs__leak_registry.entity_type` -> `STRING`
- `obs__leak_registry.entity_key` -> `STRING`
- `obs__leak_registry.leak_type` -> `STRING`
- `obs__leak_registry.severity_score` -> `DOUBLE`
- `obs__leak_registry.supporting_metrics` -> `STRING` (JSON-encoded)
- `obs__leak_registry.confidence_note` -> `STRING`
- `obs__leak_registry.recommended_action` -> `STRING`
- `obs__leak_registry.upstream_model_refs` -> `STRING` (JSON-encoded)

- `obs__priority_actions.priority_rank` -> `BIGINT`
- `obs__priority_actions.period_start` -> `DATE`
- `obs__priority_actions.entity_type` -> `STRING`
- `obs__priority_actions.entity_key` -> `STRING`
- `obs__priority_actions.leak_type` -> `STRING`
- `obs__priority_actions.recommended_action` -> `STRING`
- `obs__priority_actions.expected_leak_reduction` -> `DOUBLE`
- `obs__priority_actions.severity_score` -> `DOUBLE`

- `obs__executive_scorecard.period_start` -> `DATE`
- `obs__executive_scorecard.total_leak_findings` -> `BIGINT`
- `obs__executive_scorecard.high_severity_findings` -> `BIGINT`
- `obs__executive_scorecard.average_severity_score` -> `DOUBLE`
- `obs__executive_scorecard.projected_value_recovery_usd` -> `DOUBLE`

- `obs__leak_explainers.period_start` -> `DATE`
- `obs__leak_explainers.entity_type` -> `STRING`
- `obs__leak_explainers.entity_key` -> `STRING`
- `obs__leak_explainers.leak_type` -> `STRING`
- `obs__leak_explainers.explainer_text` -> `STRING`
- `obs__leak_explainers.supporting_metrics` -> `STRING` (JSON-encoded)
- `obs__leak_explainers.upstream_model_refs` -> `STRING` (JSON-encoded)
- `obs__leak_explainers.confidence_note` -> `STRING`

## Completion standard

This iteration is complete only when a human can look at the observatory outputs and understand what is going wrong, why it matters, and what should be done first.

## Prophecy implementation reminder

- The model names listed above are logical shortnames.
- Physical model names and filenames must follow `<pipeline_name>__<model_shortname>`.
- For this iteration, the physical pipeline name is `observatory_reporting`.
- Cross-pipeline inputs from the diagnostic pipelines must arrive through source entries, not direct canvas or `ref()` reuse.
- Keep CTE names globally unique across the project.
- After editing files, call `update_files()`, inspect the surviving files, and continue from the post-compile state.
