# Lineage rules

These rules are mandatory for this skill.

## 1. Internal same-pipeline dependencies

When a model consumes output from another model in the **same pipeline**, follow one of these patterns.

### Option A: Self-contained model (preferred)

If the model can derive its data directly from source tables, use `{{ source() }}` and set `input_ports = None` in `pipeline.py`. This avoids cross-model coordination and is more resilient to server restructuring.

### Option B: `ref()` with pipeline connection

If the model must consume the output of another model in the same pipeline:

1. Use `{{ ref('upstream_model_name') }}` in SQL.
2. Ensure the consuming model has `input_ports` defined in `pipeline.py`.
3. Add a `>>` connection in `pipeline.py` linking the upstream model to the consuming model's input port.

All three are required. Using `ref()` alone without the pipeline connection will cause the server to create an unresolved placeholder and enter an error state.

### Why this matters

The Prophecy server maintains consistency between:

- `{{ ref() }}` calls in SQL
- `input_ports` declarations in `pipeline.py`
- `>>` connection statements in `pipeline.py`

If these are not aligned, the server cannot resolve the dependency and will fail.

### Port-connected SQL pattern

For multi-input models, the compiled SQL may reference port-fed inputs through temporary sources such as:

```sql
WITH data_from_port_0 AS (
    SELECT * FROM {{ prophecy_tmp_source('pipeline_name', 'upstream_a') }}
),
data_from_port_1 AS (
    SELECT * FROM {{ prophecy_tmp_source('pipeline_name', 'upstream_b') }}
)
SELECT *
FROM data_from_port_0 a
JOIN data_from_port_1 b
  ON a.id = b.id
```

Do not hand-edit unresolved empty placeholders. Fix the missing `ref()` / `input_ports` / `>>` alignment instead.

## 2. Cross-pipeline handoffs must use materialized sources

When one pipeline needs outputs from another pipeline:

1. materialize the upstream handoff model as a Databricks table or view
2. create the matching `sources.yml` entry
3. read it in the downstream pipeline with `{{ source() }}`

Do **not** use `{{ ref() }}` across pipeline boundaries.

### This skill's required handoffs

- `commerce_foundation` -> `journey_diagnostics`
- `commerce_foundation` -> `product_value_diagnostics`
- `journey_diagnostics` -> `observatory_reporting`
- `product_value_diagnostics` -> `observatory_reporting`

## 3. Do not reference prior canvas gems directly across pipelines

Continuing the solution in a new pipeline by directly reusing gems from another pipeline creates fragile coupling and defeats the point of the smaller-pipeline topology.

- If the downstream model can be self-contained, read the necessary source tables directly.
- If the downstream logic truly needs upstream transformed output, materialize the handoff and consume it as a source in the new pipeline.

## 4. Prefer thin intermediate layers over giant rewrites

Each stage should build on the prior stage with narrow, named models. Do not rewrite or duplicate the earlier stage inside a later stage.

## 5. Every final finding must remain traceable

Final observatory outputs must retain references to the upstream models that support the finding.

## 6. If a model becomes a reusable table or view, also create a source entry

When a model is materialized as a persistent table or view and later consumed by downstream pipeline gems, create the matching source entry in `prophecy-sources/sources.yml`. Downstream gems should consume the source entry rather than re-executing or duplicating the model logic.

## 7. Keep logical shortnames and physical names aligned

The bundle uses logical shortnames in docs and contracts. Physical Prophecy model names must follow `<pipeline_name>__<model_shortname>`, and the same physical name must be used in the SQL file, `schema.yml`, and all `ref()` calls when `ref()` is the chosen dependency pattern.
