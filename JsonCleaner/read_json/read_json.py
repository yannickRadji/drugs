# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json

from jsoncomment import JsonComment
import azure.functions as func


def main(encoding: str, inputblob: bytes) -> str:
    parser = JsonComment(json)
    logging.info(f"@@@@@@ {encoding}! have waiting to use for devode")
    return json.dumps(parser.loads(inputblob.decode(encoding)))
