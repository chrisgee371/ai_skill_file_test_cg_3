from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "commerce_foundation", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    commerce_foundation__int__session_page_sequence = Process(
        name = "commerce_foundation__int__session_page_sequence",
        properties = ModelTransform(modelName = "commerce_foundation__int__session_page_sequence")
    )
    commerce_foundation__dim__products = Process(
        name = "commerce_foundation__dim__products",
        properties = ModelTransform(modelName = "commerce_foundation__dim__products")
    )
    commerce_foundation__stg__orders = Process(
        name = "commerce_foundation__stg__orders",
        properties = ModelTransform(modelName = "commerce_foundation__stg__orders")
    )
    commerce_foundation__stg__order_items = Process(
        name = "commerce_foundation__stg__order_items",
        properties = ModelTransform(modelName = "commerce_foundation__stg__order_items")
    )
    commerce_foundation__stg__website_pageviews = Process(
        name = "commerce_foundation__stg__website_pageviews",
        properties = ModelTransform(modelName = "commerce_foundation__stg__website_pageviews")
    )
    commerce_foundation__int__session_order_bridge = Process(
        name = "commerce_foundation__int__session_order_bridge",
        properties = ModelTransform(modelName = "commerce_foundation__int__session_order_bridge"),
        input_ports = ["in_0", "in_1"]
    )
    commerce_foundation__stg__order_item_refunds = Process(
        name = "commerce_foundation__stg__order_item_refunds",
        properties = ModelTransform(modelName = "commerce_foundation__stg__order_item_refunds")
    )
    commerce_foundation__int__session_entry_page = Process(
        name = "commerce_foundation__int__session_entry_page",
        properties = ModelTransform(modelName = "commerce_foundation__int__session_entry_page")
    )
    commerce_foundation__stg__website_sessions = Process(
        name = "commerce_foundation__stg__website_sessions",
        properties = ModelTransform(modelName = "commerce_foundation__stg__website_sessions")
    )
    commerce_foundation__int__order_item_net_value = Process(
        name = "commerce_foundation__int__order_item_net_value",
        properties = ModelTransform(modelName = "commerce_foundation__int__order_item_net_value"),
        input_ports = ["in_0", "in_1"]
    )

