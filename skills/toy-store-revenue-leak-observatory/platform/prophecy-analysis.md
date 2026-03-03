# Exposing data to Analysis

Add a Visualize gem only when the user explicitly wants the dataset exposed to Analysis dashboards or downstream visual exploration.

## Pattern

- Put the Visualize gem after a stable mart or output model.
- Use a descriptive node name because that becomes the Analysis-visible table identifier.
- Visualize typically has `input_ports=["in_0"]` and no output ports.

## Example intent

Expose `obs__executive_scorecard` or a stable mart, not an unstable intermediate model.
