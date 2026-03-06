# Mandatory Prophecy workflow

## Preflight before non-trivial SQL

1. Read `chris_demos.demos.databricks_sql_pattern_index` or the mirrored JSON file in `support_files/`.
2. Shortlist candidate pattern families by tags, shape, role, and `prophecy_skill_status`.
3. Read `chris_demos.demos.databricks_sql_pattern_registry` and `chris_demos.demos.databricks_sql_pattern_composition_rules`.
4. Open the referenced raw pattern packs under `support_files/databricks_sql_pattern_packs/` when deeper examples are needed.
5. Decide which pattern will serve as the base query or read pattern, which modifiers are needed, and whether a wrapper or follow-on DDL step is actually required.

## Edit and compile loop

1. Create or update files (`pipeline.py`, SQL models, `schema.yml`, `sources.yml` as needed).
2. Call `update_files()`.
3. Check the compilation result.
4. Fix blocking errors.
5. Re-run `update_files()`.
6. Re-open the surviving files because the server may have rewritten or consolidated them.
7. Compare the surviving implementation to the chosen pattern family again before making the next change.
8. Continue only from the post-compile state.

## Multi-pipeline execution order for this skill

1. finish `commerce_foundation`
2. validate its handoff outputs
3. materialize the handoff tables or views
4. add the matching `sources.yml` entries
5. only then start `journey_diagnostics` and `product_value_diagnostics`
6. only after those diagnostic handoffs are stable should `observatory_reporting` be built

## Important failure rule

If `update_files()` returns `success=false`, treat the attempted edit as rejected. Re-apply the fix from the last surviving state.

## Error handling rule

Ignore only known platform noise such as `connection not found`.
Fix immediately:
- syntax errors
- missing columns
- type mismatches
- duplicate CTE names
- UNION shape mismatches
- naming / file alignment issues
- missing `sources.yml` entries for cross-pipeline handoffs
- use of patterns marked `forbidden` or `negative_control`
- use of `ref()` without the matching pipeline connection

## Pattern-library reminder

The global pattern library is deliberately broader than the immediate toy-store task. Use it to reduce advanced-structure guesswork, not to force every model into the same shape. When several patterns fit, take the clearest `preferred` or `allowed` option first.
