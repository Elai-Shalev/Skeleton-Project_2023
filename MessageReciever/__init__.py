import azure.functions as func
from azure.data.tables import TableClient
import os
import json
from datetime import datetime
import logging


def main(event_req: func.In[str], # how to input bind from event hub ????????????????????????
         signalRMessages: func.Out[str],
         testEventHub: func.Out[str] #TODO(TEST EvenHUB)
         ) -> func.HttpResponse:

    connection_string = os.getenv("AzureWebJobsStorage")
    try:
        with TableClient.from_connection_string(connection_string, table_name="countertable") as table:
            entity = table.get_entity("counter", "counter_0")
            new_counter_value = entity["value"] + 1
            entity["value"] = new_counter_value
            table.update_entity(entity)

            signalRMessages.set(json.dumps({
                'target': "newMessage",
                # Array of arguments
                'arguments': [f"The counter current Value is {new_counter_value}"]
            }))
            # TODO(TEST EventHub)
            testEventHub.set(f"The counter current Value is {new_counter_value}")
            return func.HttpResponse(f"New counter Value {new_counter_value}", status_code=200)
    except Exception as e:
        logging.error(e)
        return func.HttpResponse(f"Something went wrong", status_code=500)