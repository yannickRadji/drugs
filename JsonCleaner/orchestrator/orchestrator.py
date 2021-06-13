# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "activity")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json

import azure.functions as func
import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    input_dict = context.get_input()
    logging.info(f"Start Tokyo {input_dict}")
    result1 = yield context.call_activity("read_json", input_dict.get("encoding", "utf-8"))
    logging.info(f"Result Tokyo = '{result1}'.")

    return result1

    # logging.info(f"Start Seattle")
    # result2 = yield context.call_activity("activity", "Seattle")
    # logging.info(f"Result Seattle = '{result2}'.")

    # logging.info(f"Start London")
    # result3 = yield context.call_activity("activity", "London")
    # logging.info(f"Result London = '{result3}'.")

    # logging.info(f"Result final = '{[result1, result2, result3]}'.")
    # return [result1, result2, result3]

main = df.Orchestrator.create(orchestrator_function)