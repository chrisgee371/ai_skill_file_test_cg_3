# sources.yml guidance

If a model is materialized as a persistent table or view and is then reused downstream on the canvas, create a matching `sources.yml` entry.

Why: downstream pipeline gems should consume the source entry instead of re-executing or duplicating the model logic.

This is especially important when the server reshapes the graph and when persistent handoff points are needed between stages or between canvas sections.
