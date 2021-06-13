# This function an HTTP starter function for Durable Functions.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable activity function (default name is "Hello")
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt
 
import logging
import traceback

import azure.functions as func
import azure.durable_functions as df


async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    try:
        orchest_name = "functionName"
        client = df.DurableOrchestrationClient(starter)

        called_orchest = req.route_params.get(orchest_name)

        client_input = None
        if req.get_body():
            client_input = req.get_json()
        if req.params:
            client_input = dict(req.params)
        if req.params and req.get_body():
            logging.warn("Do not support query params & body in the same time")

        instance_id = await client.start_new(called_orchest, None, client_input)

        logging.info(f"Started orchestration with ID = '{instance_id}'.")

        return client.create_check_status_response(req, instance_id)

    except Exception as e:
        str_issue = "Client initialisation Issue: {}".format(e)
        logging.error(str_issue, traceback.format_exc())
        return func.HttpResponse(
                    str_issue,
                    status_code=500
                )
