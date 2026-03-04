# When to split vs combine models

## Split into multiple models when

- the data branches to different downstream targets
- an intermediate dataset must be exposed through Visualize
- different materializations are required
- a step is easier to validate as an independent contract point

## Keep one model when

- the logic is a simple linear chain
- the user wants a compact DAG
- there is no branching or reuse pressure

Prefer the smallest number of models that still keeps lineage, debugging, and server behavior manageable.

## Prefer several smaller pipelines for this skill

This skill deliberately uses **four pipelines** instead of one giant Prophecy canvas:

- `commerce_foundation`
- `journey_diagnostics`
- `product_value_diagnostics`
- `observatory_reporting`

Use a new pipeline when:
- the logic belongs to a different business domain
- a stable handoff point is needed for downstream reuse
- the upstream outputs should be materialized and source-declared
- keeping everything together would create an oversized or fragile graph

## If you split models inside one pipeline: ensure the connection exists

When a downstream model references an upstream model in the **same pipeline**:

- the downstream model needs `input_ports` in `pipeline.py`
- a `>>` connection must link the upstream output to the downstream input
- the SQL must use `{{ ref('upstream_model_name') }}`

All three must be present. If the server creates `input_ports` during auto-splitting, verify that matching `>>` connections also exist. Missing connections cause unresolved placeholders and compilation failure.

## If you split work across pipelines: use materialized handoffs

When a downstream pipeline depends on an upstream pipeline:

1. materialize the upstream handoff model as a table or view
2. create the matching `sources.yml` entry
3. consume it downstream with `{{ source() }}`

Do not use cross-pipeline `ref()`.

## Prefer self-contained models

When possible, keep models self-contained by reading from `{{ source() }}` with `input_ports = None`. This avoids the `ref()` + connection coordination requirement and is more resilient to server restructuring.

## Same-pipeline port connection example

```python
join_model = Process(
    name="pipeline__join_model",
    properties=ModelTransform(modelName="pipeline__join_model"),
    input_ports=["in_0", "in_1"]
)

source_a >> join_model._in(0)
source_b >> join_model._in(1)
```

```sql
WITH data_from_port_0 AS (
    SELECT * FROM {{ prophecy_tmp_source('pipeline_name', 'source_a') }}
),
data_from_port_1 AS (
    SELECT * FROM {{ prophecy_tmp_source('pipeline_name', 'source_b') }}
)
SELECT *
FROM data_from_port_0 a
JOIN data_from_port_1 b
  ON a.id = b.id
```

## Cross-pipeline handoff example

- upstream physical model: `commerce_foundation__int__session_order_bridge`
- materialized in Databricks as a table or view
- declared in `sources.yml`
- consumed downstream in `journey_diagnostics` or `product_value_diagnostics` through `{{ source() }}`
