# Prophecy SQL rules

## CTE names must be globally unique

All CTE names must be unique across the entire project, not just within one model. The Prophecy server can flatten or consolidate models, and duplicate CTE names can then collide.

Pattern:
- bad: `source_data`, `filtered`, `ranked`, `final`
- good: `sess_entry_source`, `sess_entry_ranked`, `obs_leak_scored`

## UNION / INTERSECT / EXCEPT should combine pre-shaped CTEs

Do not change shape directly inside set-operation branches. Build branch-specific CTEs first, then combine them with `SELECT *`.

Bad:
```sql
SELECT 'A' AS bucket, metric FROM table_a
UNION ALL
SELECT 'B' AS bucket, metric FROM table_b
```

Good:
```sql
WITH bucket_a AS (
  SELECT 'A' AS bucket, metric FROM table_a
),
bucket_b AS (
  SELECT 'B' AS bucket, metric FROM table_b
),
combined AS (
  SELECT * FROM bucket_a
  UNION ALL
  SELECT * FROM bucket_b
)
SELECT * FROM combined
```

## One major clause per CTE

Keep each CTE focused on one primary operation whenever possible:
- filter
- join
- aggregate
- rank
- project / rename

This makes server consolidation safer and debugging faster.

## Final select should be a clean handoff

Prefer a final CTE or final `SELECT *` from the last named step instead of a long unstructured tail query.

## Explicitly cast count-style outputs to BIGINT

Prophecy/Spark can infer different integer widths depending on the expression pattern. To prevent
schema drift (especially across server auto-splitting and consolidation), explicitly cast all
count-style outputs to `BIGINT`.

Use this pattern:

```sql
CAST(<expr> AS BIGINT) AS <count_column>
```

Examples:

```sql
CAST(COUNT(*) AS BIGINT) AS sessions
CAST(SUM(CASE WHEN order_id IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT) AS conversions
```

For this project, the authoritative list of required count-style columns and types is:
- `chris_demos.demos.expected_column_types` (support table)
- `skills/toy-store-revenue-leak-observatory/contracts/expected_column_types.json` (repo file)

Those count columns are a subset of the full model-level datatype contract in `model_output_schemas`.


## All named output fields must match the datatype contract

For every model in this project, make the final projection match the authoritative datatype contract in:
- `chris_demos.demos.model_output_schemas` (support table)
- `skills/toy-store-revenue-leak-observatory/contracts/model_output_schemas.json` (repo file)

Use explicit casts in the final projection for:
- `BIGINT` counts, ids, and rank columns
- `DOUBLE` money, rate, and score columns
- `DATE` reporting and comparison-window columns
- `TIMESTAMP` raw event timestamps
- `STRING` descriptive and action columns

Structured observatory payloads such as `supporting_metrics` and `upstream_model_refs` must be emitted as STRING-encoded JSON unless a later platform-safe nested type contract is introduced.
