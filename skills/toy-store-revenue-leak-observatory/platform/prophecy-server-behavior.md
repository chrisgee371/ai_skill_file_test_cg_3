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
3. adjust connections, refs, and expectations to the server’s final structure

Do not assume the pre-compile local shape survived unchanged.
