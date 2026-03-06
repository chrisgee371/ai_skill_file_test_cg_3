# Forbidden patterns

Do not do any of the following.

1. Guess joins or keys that are not supported by the `chris_demos.demos.source_manifest` table or the Databricks profile tables.
2. Use `{{ ref() }}` without adding the corresponding `>>` pipeline connection.
3. Reference prior canvas gems directly when continuing the solution in a different pipeline.
4. Collapse the whole solution into one giant pipeline or one giant query.
5. Discard `NULL` UTM values without explicit justification.
6. Compare page or campaign variants outside their valid overlap windows.
7. Ignore refunds when computing net value.
8. Over-engineer product hierarchy logic; this dataset only has four products.
9. Pretend chronology does not matter. Product launches and page variants are introduced over time.
10. Emit severity scores without a supporting metric trail.
11. Reuse generic CTE names such as `source_data`, `filtered`, `ranked`, or `final` anywhere in the project.
12. Put shape-changing expressions directly inside `UNION`, `INTERSECT`, or `EXCEPT` branches; pre-shape each branch first and combine them with `SELECT *`.
13. Pack multiple major clauses into one large CTE when the same logic can be split into smaller named steps.
14. Let SQL filenames, model names, and references drift apart.
15. Skip `update_files()` after code changes.
16. Assume the Prophecy server will preserve your exact model boundaries after compile.
17. Add Visualize gems everywhere by default; only add them when downstream Analysis exposure is actually required.
18. Assume that `{{ ref() }}` alone is sufficient for internal model dependencies; the matching pipeline connection is also required.
19. Use cross-pipeline `{{ ref() }}` when a materialized handoff plus `sources.yml` is the required pattern.
20. Start building `journey_diagnostics`, `product_value_diagnostics`, or `observatory_reporting` before the upstream handoff tables are stable.
21. Author non-trivial SQL from memory before checking the pattern index, registry, composition rules, and raw pack files.
22. Emit any library entry where `suitable_for_model_production = false`, `negative_control = true`, or `prophecy_skill_status = 'forbidden'`.
23. Choose a pattern marked `avoid` when a `preferred` or `allowed` pattern already solves the problem.
24. Use `NATURAL JOIN` by default; explicit join keys are required for reviewable lineage.
25. Prefer `LATERAL VIEW` when a table-valued-function form in `FROM` gives the same result more clearly.
26. Treat follow-on metadata DDL such as tagging as if it were the whole transformation body.
27. Assume caution patterns are universally available; check feature support first for federation, AI functions, Delta history/CDF, and materialized views.

## Clarifications

- `{{ source() }}` is safe and preferred for self-contained models that read directly from declared sources.
- `{{ ref() }}` is safe only when the consumer has a matching pipeline graph connection.
- The pattern library is broad on purpose. Coverage does not imply default preference.
