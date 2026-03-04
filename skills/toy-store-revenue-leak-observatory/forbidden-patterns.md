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

## Clarifications

- `{{ source() }}` is safe and preferred for self-contained models that read directly from external tables.
- If a model must consume the output of another model in the same pipeline, use `{{ ref() }}` together with matching `input_ports` and `>>` connections.
- If a model must consume the output of a model in another pipeline, materialize the upstream handoff and consume it via `{{ source() }}`.
- Do not try to wire canvas gems together from SQL without the corresponding pipeline graph connection.
