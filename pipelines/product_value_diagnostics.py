from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "product_value_diagnostics", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    product_value_diagnostics__diag__basket_leak = Process(
        name = "product_value_diagnostics__diag__basket_leak",
        properties = ModelTransform(modelName = "product_value_diagnostics__diag__basket_leak"),
        input_ports = ["in_0", "in_1"]
    )
    product_value_diagnostics__diag__launch_leak = Process(
        name = "product_value_diagnostics__diag__launch_leak",
        properties = ModelTransform(modelName = "product_value_diagnostics__diag__launch_leak"),
        input_ports = ["in_0", "in_1"]
    )
    product_value_diagnostics__diag__refund_leak = Process(
        name = "product_value_diagnostics__diag__refund_leak",
        properties = ModelTransform(modelName = "product_value_diagnostics__diag__refund_leak")
    )

