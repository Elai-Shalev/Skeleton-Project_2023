import azure.functions as func
from azure.data.tables import TableClient

import os 

def main(req: func.HttpRequest) -> func.HttpResponse:
    connection_string = os.getenv("AzureWebJobsStorage")

    try:
        with TableClient.from_connection_string(connection_string, table_name="countertable") as table:
            entity = table.get_entity("counters", "counter_0")
            counter_value = entity["value"]
            return func.HttpResponse(f"Counter Value {counter_value}", status_code=200)
    except:
        return func.HttpResponse(f"Something went wrong", status_code=500)

