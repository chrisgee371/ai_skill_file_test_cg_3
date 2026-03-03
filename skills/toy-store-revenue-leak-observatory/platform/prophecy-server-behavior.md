# Prophecy server behavior

## Auto-consolidation can happen

The server may consolidate a linear chain of models into fewer files or nodes.

## Auto-splitting can happen

If a CTE is reused or the compiler sees an opportunity to reshape the graph, it may split logic into separate generated models.

## Model deletion can happen

If a SQL file is not actually referenced by the pipeline, or if the model name and the file name drift apart, the server can remove the model during compile/update.

## Practical rule

After every `update_files()` call:

1. inspect the surviving files
2. inspect the surviving graph shape
3. verify that any `{{ ref() }}` calls have matching `>>` connections in `pipeline.py`
4. adjust connections, refs, and expectations to the server's final structure

Do not assume the pre-compile local shape survived unchanged.

## Port and connection consistency

The server enforces strict consistency between:

- `input_ports` declarations in `pipeline.py`
- `>>` connection statements in `pipeline.py`
- `{{ ref() }}` calls in SQL models

If a model uses `{{ ref() }}` but no corresponding `>>` connection exists, the server will:

1. replace the `ref()` with an unresolved placeholder
2. create `input_ports` expecting data that never arrives
3. lock the model into an unrecoverable error state

To avoid this:

- whenever you add a `ref()`, also add the matching connection
- whenever the server creates `input_ports`, verify that connections exist to feed them

## Recovery when the server enforces inconsistent state

Sometimes the server's internal graph state becomes inconsistent. Symptoms include:

- unresolved placeholders or empty backticks in generated SQL
- the server reverts your changes back to a broken state
- the server recreates deleted models with the same broken structure
- the server removes connections you add

Recovery options, in order of preference:

1. Make the model self-contained by reading from `{{ source() }}` and removing the port dependency.
2. Rename the model if the server appears to have corrupted state tied to one model name.
3. Delete and recreate the affected pipeline if corruption is widespread.
4. Escalate to Prophecy support if the server continues to enforce the broken state.
