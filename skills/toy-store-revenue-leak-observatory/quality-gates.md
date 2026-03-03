# Quality gates

A stage is not complete until its quality gates are satisfied.

## Global gates

- The stage uses Databricks-compatible SQL.
- All internally generated upstream dependencies use `{{ ref('model_name') }}`.
- Model names follow the naming conventions in `naming-conventions.md`.
- Physical model names follow `<pipeline_name>__<model_shortname>` consistently.
- SQL filenames, model names, and refs all match.
- CTE names are globally unique across the project.
- No guessed columns, joins, or unexplained filters are introduced.
- The stage output grain matches `chris_demos.demos.model_registry`.
- `update_files()` completed successfully after the latest change set.
- Only non-blocking platform noise such as `connection not found` may be ignored; syntax, missing column, type mismatch, duplicate CTE, and UNION-shape errors must be fixed.
- If any model is materialized as a table or view and then reused downstream on the canvas, the corresponding `sources.yml` entry exists.
- If ephemeral models are created, the required `schema.yml` entries exist.

## Stage 1

- Source row counts match the supplied Databricks source tables exactly.
- Source primary keys are unique.
- Profile and integrity tables were read before model generation.

## Stage 2

- All intermediate models declare and respect a clear grain.
- Session, order, item, and refund joins are lineage-safe and orphan-free.
- `int__order_item_net_value` computes net value after refund.

## Stage 3

- Variant analyses are chronology-aware.
- `NULL` UTM traffic is retained as a meaningful class unless explicitly excluded.
- Diagnostic severity scores are bounded and explainable.

## Stage 4

- Product analysis stays lightweight and respects the four-product scope.
- Refund-aware metrics use refund timestamps and values correctly.
- Launch windows do not begin before a product launch date.

## Stage 5

- Every observatory record is explainable.
- Every ranked action maps back to supporting metrics and upstream models.
- Final outputs do not hide uncertainty.
- Add a Visualize gem only if the stage is explicitly meant to expose data to Analysis.
