# Prophecy SQL rules

## Start from the pattern library, not a blank page

Before writing non-trivial SQL:

1. read the pattern index
2. shortlist candidate pattern families
3. read the registry entries and composition rules
4. open the referenced raw pattern packs from disk when needed

Do not improvise advanced SQL structures from memory when an equivalent governed pattern already exists.

## Choose the right pattern role

- `base_query` - the main transformation body
- `read_pattern` - a source, system, time-travel, CDF, federation, or TVF-driven read that should usually land in its own source CTE first
- `clause_modifier` - a modifier that decorates a base query
- `materialization_wrapper` - a create/replace statement that wraps a finished inner query
- `post_create_augmentation` - follow-on metadata DDL such as tagging
- `negative_control` - retained only as a signal for what not to emit as a standalone model

## CTE names must be globally unique

All CTE names must be unique across the entire project, not just within one model. The Prophecy server can flatten or consolidate models, and duplicate CTE names can then collide.

Pattern:
- bad: `source_data`, `filtered`, `ranked`, `final`
- good: `sess_entry_source`, `sess_entry_ranked`, `obs_leak_scored`

## Prefer one major clause per CTE

Keep each CTE focused on one primary operation whenever possible:
- read
- filter
- join
- aggregate
- rank
- project / rename

This makes server consolidation safer and debugging faster.

## Read patterns should become named source CTEs

When you use a read pattern such as:
- time travel
- `table_changes`
- Delta change feed consumption
- federation reads
- `information_schema`
- generator-heavy TVF reads

land that pattern in a named source CTE first. Shape, filter, and join it later in clearly named steps.

## Modifier patterns must decorate a shaped base query

Patterns such as:
- `ORDER BY`
- `SORT BY`
- `DISTRIBUTE BY`
- `CLUSTER BY`
- `LIMIT`
- `OFFSET`
- `DISTINCT`
- query hints

should be attached to a shaped base query. Do not let them replace grain definition or conceal unresolved duplication problems.

## Wrapper statements should wrap the finished inner query

CTAS, views, materialized views, full-refresh replacement, and table-property wrappers should surround a complete inner query that already has:

- named CTEs
- explicit typing where needed
- a clean final projection

DDL does not remove the need for a disciplined query body.

## Follow-on DDL should remain follow-on DDL

Patterns such as `SET TAG` are useful, but they are not the model body. Apply them after the target relation exists, or keep them as a clearly separate step in the same change set.

## Prefer `source()` authoring and use `ref()` intentionally

When possible, author models as self-contained `source()`-driven SQL.

Use `ref()` only when reusing a same-pipeline model output and only when all three conditions are present:
- `ref()` in SQL
- `input_ports` on the consumer
- a matching `>>` pipeline connection

Never use cross-pipeline `ref()`.

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

## Handle caution and avoid patterns deliberately

- `caution` means the pattern is valid but feature availability, performance, or platform behavior may matter.
- `avoid` means the syntax is valid but there is usually a clearer choice in this skill context.
- `forbidden` means the entry is present only as negative guidance and must not be emitted as a standalone model.
