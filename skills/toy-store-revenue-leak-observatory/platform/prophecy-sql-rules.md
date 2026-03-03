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
