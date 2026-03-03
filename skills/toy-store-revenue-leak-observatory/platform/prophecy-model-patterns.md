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

## If you split: ensure the connection exists

When a downstream model references an upstream model:

- the downstream model needs `input_ports` in `pipeline.py`
- a `>>` connection must link the upstream output to the downstream input
- the SQL must use `{{ ref('upstream_model_name') }}`

All three must be present. If the server creates `input_ports` during auto-splitting, verify that matching `>>` connections also exist. Missing connections cause unresolved placeholders and compilation failure.

## Prefer self-contained models

When possible, keep models self-contained by reading from `{{ source() }}` with `input_ports = None`. This avoids the `ref()` + connection coordination requirement and is more resilient to server restructuring.

## Port connection example

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
