import logging
import json

from jsoncomment import JsonComment
import azure.functions as func


def main(triggerblob: func.InputStream, outputblob: func.Out[str]):
    
    parser = JsonComment(json)

    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {triggerblob.name}\n"
                 f"Blob Size: {triggerblob.length} bytes")

    raw_text = triggerblob.read().decode("utf-8")
    py_obj = parser.loads(raw_text)
    str_parsed = json.dumps(py_obj)
    outputblob.set(str_parsed)
