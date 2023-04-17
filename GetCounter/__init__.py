import azure.functions as func
import json
import logging

def main(req: func.HttpRequest, counterTable) -> func.HttpResponse:
    listOfTableRowsAsJSON = json.loads(counterTable)
    # Since our table has only one counter, lets read first entry
    val = listOfTableRowsAsJSON[0]['value']
    return func.HttpResponse(f"Current counter value is {val}", status_code=200)