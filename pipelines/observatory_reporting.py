from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "observatory_reporting", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    observatory_reporting__obs__executive_scorecard = Process(
        name = "observatory_reporting__obs__executive_scorecard",
        properties = ModelTransform(modelName = "observatory_reporting__obs__executive_scorecard"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    observatory_reporting__obs__leak_explainers = Process(
        name = "observatory_reporting__obs__leak_explainers",
        properties = ModelTransform(modelName = "observatory_reporting__obs__leak_explainers"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    observatory_reporting__obs__leak_registry = Process(
        name = "observatory_reporting__obs__leak_registry",
        properties = ModelTransform(modelName = "observatory_reporting__obs__leak_registry"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    observatory_reporting__obs__priority_actions = Process(
        name = "observatory_reporting__obs__priority_actions",
        properties = ModelTransform(modelName = "observatory_reporting__obs__priority_actions"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )

