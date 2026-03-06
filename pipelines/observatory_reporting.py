from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "observatory_reporting", version = 1)

with Pipeline(args) as pipeline:
    observatory_reporting__obs_exp_leak_definitions_from_003 = Process(
        name = "observatory_reporting__obs_exp_leak_definitions_from_003",
        properties = ModelTransform(modelName = "observatory_reporting__obs_exp_leak_definitions_from_003"),
        input_ports = None
    )
    observatory_reporting__obs_exp_leak_definitions_from_000 = Process(
        name = "observatory_reporting__obs_exp_leak_definitions_from_000",
        properties = ModelTransform(modelName = "observatory_reporting__obs_exp_leak_definitions_from_000"),
        input_ports = None
    )
    observatory_reporting__obs__priority_actions = Process(
        name = "observatory_reporting__obs__priority_actions",
        properties = ModelTransform(modelName = "observatory_reporting__obs__priority_actions"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    observatory_reporting__obs__executive_scorecard = Process(
        name = "observatory_reporting__obs__executive_scorecard",
        properties = ModelTransform(modelName = "observatory_reporting__obs__executive_scorecard"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    observatory_reporting__obs__leak_registry = Process(
        name = "observatory_reporting__obs__leak_registry",
        properties = ModelTransform(modelName = "observatory_reporting__obs__leak_registry"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    obs_reg_experiment_findings = Process(
        name = "obs_reg_experiment_findings",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "journey_diagnostics__diag__experiment_findings",
            sourceType = "Table",
            sourceName = "chris_demos.demos"
          )
        ),
        input_ports = None
    )
    observatory_reporting__obs_exp_leak_definitions_from_004 = Process(
        name = "observatory_reporting__obs_exp_leak_definitions_from_004",
        properties = ModelTransform(modelName = "observatory_reporting__obs_exp_leak_definitions_from_004"),
        input_ports = None
    )
    observatory_reporting__obs__leak_explainers = Process(
        name = "observatory_reporting__obs__leak_explainers",
        properties = ModelTransform(modelName = "observatory_reporting__obs__leak_explainers"),
        input_ports = None
    )
    observatory_reporting__obs_exp_leak_definitions_from_002 = Process(
        name = "observatory_reporting__obs_exp_leak_definitions_from_002",
        properties = ModelTransform(modelName = "observatory_reporting__obs_exp_leak_definitions_from_002"),
        input_ports = None
    )

