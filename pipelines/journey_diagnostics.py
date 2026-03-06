from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "journey_diagnostics", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    journey_diagnostics__diag__journey_leak = Process(
        name = "journey_diagnostics__diag__journey_leak",
        properties = ModelTransform(modelName = "journey_diagnostics__diag__journey_leak"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    journey_diagnostics__diag__experiment_findings = Process(
        name = "journey_diagnostics__diag__experiment_findings",
        properties = ModelTransform(modelName = "journey_diagnostics__diag__experiment_findings"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5"]
    )
    journey_diagnostics__jd_lpd_final = Process(
        name = "journey_diagnostics__jd_lpd_final",
        properties = ModelTransform(modelName = "journey_diagnostics__jd_lpd_final"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    journey_diagnostics__diag__acquisition_leak = Process(
        name = "journey_diagnostics__diag__acquisition_leak",
        properties = ModelTransform(modelName = "journey_diagnostics__diag__acquisition_leak"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    journey_diagnostics__jd_chkd_final = Process(
        name = "journey_diagnostics__jd_chkd_final",
        properties = ModelTransform(modelName = "journey_diagnostics__jd_chkd_final"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    journey_diagnostics__jd_cvd_final = Process(
        name = "journey_diagnostics__jd_cvd_final",
        properties = ModelTransform(modelName = "journey_diagnostics__jd_cvd_final"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    journey_diagnostics__jd_fsd_final = Process(
        name = "journey_diagnostics__jd_fsd_final",
        properties = ModelTransform(modelName = "journey_diagnostics__jd_fsd_final"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    journey_diagnostics__jd_tsd_final = Process(
        name = "journey_diagnostics__jd_tsd_final",
        properties = ModelTransform(modelName = "journey_diagnostics__jd_tsd_final"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5"]
    )

