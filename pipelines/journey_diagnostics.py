from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "journey_diagnostics", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    journey_diagnostics__diag__acquisition_leak = Process(
        name = "journey_diagnostics__diag__acquisition_leak",
        properties = ModelTransform(modelName = "journey_diagnostics__diag__acquisition_leak"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    journey_diagnostics__diag__journey_leak = Process(
        name = "journey_diagnostics__diag__journey_leak",
        properties = ModelTransform(modelName = "journey_diagnostics__diag__journey_leak"),
        input_ports = ["in_0", "in_1"]
    )
    journey_diagnostics__diag__experiment_findings = Process(
        name = "journey_diagnostics__diag__experiment_findings",
        properties = ModelTransform(modelName = "journey_diagnostics__diag__experiment_findings"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8"]
    )

