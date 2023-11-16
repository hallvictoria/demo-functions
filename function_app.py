import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
import time
import csv
import json


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.blob_trigger(arg_name="client",
                path="test-input/{name}",
                connection="AzureWebJobsStorage")
@app.blob_output(arg_name="$return",
                 path="test-output/deferred-binding-output.json",
                 connection="AzureWebJobsStorage")
def blobTrigger(client: func.ConnectionInfo) -> str:
    start_time = time.time()

    # creating a client
    blob_service_client = BlobServiceClient.from_connection_string(client.connection)
    blob_client = blob_service_client.get_blob_client(container=client.container_name, blob=client.blob_name)
    csv_file = blob_client.download_blob(encoding='utf-8').readall()
    logging.info(f"--- With deferred bindings, the function finished reading in {time.time() - start_time} seconds ---")

    # Convert the CSV into a JSON object
    json_file = convert_to_json(csv_file)

    # Log the time it took to run the function
    logging.info(f"--- With deferred bindings, the function executed in {time.time() - start_time} seconds ---")

    return json_file


@app.blob_trigger(arg_name="blob",
                path="test-input/{name}",
                connection="AzureWebJobsStorage")
@app.blob_output(arg_name="$return",
                 path="test-output/current-state-output.txt",
                 connection="AzureWebJobsStorage")

def blobTriggerOld(blob: func.InputStream) -> str:
    start_time = time.time()

    downloaded_file = blob.read().decode('utf-8-sig')
    logging.info(f"--- With the current state, the function finished reading in {time.time() - start_time} seconds ---")

    json_file = convert_to_json(downloaded_file)

    # Log the time it took to run the function
    logging.info(f"--- With the current state, the function executed in {time.time() - start_time} seconds ---")

    return json_file


def convert_to_json(csv_file):
    data = []

    csv_reader = csv.DictReader(csv_file)
    for rows in csv_reader:
        data.append(rows)
    
    # # Write the data to a JSON file
    json_file = json.dumps(data, indent=4)

    return json_file
