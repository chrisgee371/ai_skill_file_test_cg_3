# Iteration 05 - Observatory and action layer

## Objective

Turn the leak diagnostics into a unified decision layer that can be consumed by downstream users without reading every mart individually.

## Read these resources first


- `skills/toy-store-revenue-leak-observatory/platform/prophecy-workflow.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-sql-rules.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-naming.md`

- `chris_demos.demos.model_registry`
- `chris_demos.demos.metric_catalog`
- `chris_demos.demos.acceptance_tests`

## Inputs

Use stage-3 and stage-4 diagnostic models via `{{ ref('model_name') }}`.

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

## Completion standard

This iteration is complete only when a human can look at the observatory outputs and understand what is going wrong, why it matters, and what should be done first.


## Prophecy implementation reminder

- The model names listed above are logical shortnames.
- Physical model names and filenames must follow `<pipeline_name>__<model_shortname>`.
- Keep CTE names globally unique across the project.
- After editing files, call `update_files()`, inspect the surviving files, and continue from the post-compile state.
