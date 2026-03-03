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
