import azure.functions as func
from azure.data.tables import TableClient
import os
import logging
import json
from datetime import datetime
import logging


def main(event: func.EventHubEvent,
 signalRMessages: func.Out[str]) -> func.HttpResponse:
    logging.info(f'Function triggered to process a message: {event.get_body().decode()}')
    logging.info(f'  EnqueuedTimeUtc = {event.enqueued_time}')
    logging.info(f'  SequenceNumber = {event.sequence_number}')
    logging.info(f'  Offset = {event.offset}')

    # Metadata
    for key in event.metadata:
        logging.info(f'Metadata: {key} = {event.metadata[key]}')

    # Handeling Table Information
    table_connection_string = os.getenv("AzureWebJobsStorage")
    try:
        with TableClient.from_connection_string(table_connection_string, table_name="countertable") as table:
            entity = table.get_entity("counter", "counter_0")
            new_counter_value = entity["value"] + 1
            entity["value"] = new_counter_value
            table.update_entity(entity)

            signalRMessages.set(json.dumps({
                'target': "newMessage",
                # Array of arguments
                'arguments': [f"The counter current Value is {new_counter_value}"]
            }))
            return func.HttpResponse(f"New counter Value {new_counter_value}", status_code=200)
    except Exception as e:
        logging.error(e)
        return func.HttpResponse(f"Something went wrong", status_code=500)

    #testing