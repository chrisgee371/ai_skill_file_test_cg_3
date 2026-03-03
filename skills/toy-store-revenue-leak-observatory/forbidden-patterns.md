# Forbidden patterns

Do not do any of the following.

1. Guess joins or keys that are not supported by the `chris_demos.demos.source_manifest` table or the Databricks profile tables.
2. Use `{{ source() }}` for models created in earlier stages of the same pipeline.
3. Reference prior canvas gems directly when continuing the same staged pipeline.
4. Collapse the whole pipeline into one giant query.
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
