# schema.yml guidance

If the build creates ephemeral models, add the required `schema.yml` entries for those physical model names.

Pattern:

```yaml
models:
  - name: "<pipeline_name>__<model_shortname>"
    tags:
      - "prophecy_temp"
```

Keep the schema entry aligned with the physical model name, not the logical shortname.
