from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "product_value_diagnostics", version = 1)

with Pipeline(args) as pipeline:
    product_value_diagnostics__pvd_lam_final = Process(
        name = "product_value_diagnostics__pvd_lam_final",
        properties = ModelTransform(modelName = "product_value_diagnostics__pvd_lam_final"),
        input_ports = ["in_0", "in_1"]
    )
    product_value_diagnostics__diag__basket_leak = Process(
        name = "product_value_diagnostics__diag__basket_leak",
        properties = ModelTransform(modelName = "product_value_diagnostics__diag__basket_leak"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    product_value_diagnostics__pvd_ppm_final = Process(
        name = "product_value_diagnostics__pvd_ppm_final",
        properties = ModelTransform(modelName = "product_value_diagnostics__pvd_ppm_final"),
        input_ports = ["in_0", "in_1"]
    )
    product_value_diagnostics__pvd_bam_final = Process(
        name = "product_value_diagnostics__pvd_bam_final",
        properties = ModelTransform(modelName = "product_value_diagnostics__pvd_bam_final"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    product_value_diagnostics__diag__refund_leak = Process(
        name = "product_value_diagnostics__diag__refund_leak",
        properties = ModelTransform(modelName = "product_value_diagnostics__diag__refund_leak"),
        input_ports = ["in_0", "in_1"]
    )
    product_value_diagnostics__diag__launch_leak = Process(
        name = "product_value_diagnostics__diag__launch_leak",
        properties = ModelTransform(modelName = "product_value_diagnostics__diag__launch_leak"),
        input_ports = ["in_0", "in_1"]
    )
    product_value_diagnostics__pvd_rpm_final = Process(
        name = "product_value_diagnostics__pvd_rpm_final",
        properties = ModelTransform(modelName = "product_value_diagnostics__pvd_rpm_final"),
        input_ports = ["in_0", "in_1"]
    )

