# Lineage rules

These rules are mandatory for this skill.

## 1. Internal staged dependencies must use ref()

If a new phase uses any objects as inputs that are outputs of previous phases in the same pipeline, the SQL model should reference those upstream models using:

```sql
{{ ref('model_name') }}
```

and **not**:

```sql
{{ source() }}
```

This preserves model lineage and prevents disconnected duplicate DAG fragments.

## 2. Do not reference prior canvas gems directly

Continuing a multi-iteration pipeline by directly reusing earlier gems can cause duplicate copies of shared objects to appear with broken lineage. Use the underlying SQL models instead.

## 3. Prefer thin intermediate layers over giant rewrites

Each stage should build on the prior stage with narrow, named models. Do not rewrite or duplicate the earlier stage inside a later stage.

## 4. Every final finding must remain traceable

Final observatory outputs must retain references to the upstream models that support the finding.

## 5. If a model becomes a reusable table or view, also create a source entry

When a model is materialized as a persistent table or view and later consumed by downstream pipeline gems, create the matching source entry in `prophecy-sources/sources.yml`. Downstream gems should consume the source entry rather than re-executing or duplicating the model logic.

## 6. Keep logical shortnames and physical names aligned

The bundle uses logical shortnames in docs and contracts. Physical Prophecy model names must follow `<pipeline_name>__<model_shortname>`, and the same physical name must be used in the SQL file, schema.yml, and all `ref()` calls.
