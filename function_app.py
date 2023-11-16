import azure.functions as func
import logging
from aiohttp import web
from azure.storage.blob.aio import BlobServiceClient
import asyncio
import datetime

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

async def stream_upload(req, blob_client):
    block_size = 1024 * 1024 * 1  # 1 MB block size, choose your block size based on your requirements
    size = 0
    # content = bytearray()
    chunk_count = 0

    # Continuously read and upload chunks of data from the request content
    while True:
        chunk = await req.content.read(block_size)
        if not chunk:
            break
        chunk_count += 1
        size += len(chunk)
        
        # Append the current chunk to the blob
        await blob_client.append_block(chunk)
        print(f"Received chunk {chunk_count}: {len(chunk)} bytes")

@app.route(route="h1", methods=['POST'])
async def http_trigger(req):
    # create the blob client
    service = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=httpstream;AccountKey=a+oM1GSaM1N7JVfdCTDaDa0uERUtjc9rtsOrNd8dvnktNKg2EVWH6vnsXHXJfVfslaO/cHanCrfM+AStwYxuAg==;EndpointSuffix=core.windows.net")

    container_name = "streamingtest"
    blob_client = service.get_blob_client(container=container_name, blob="funclogs.csv")
    
    # Create or ensure the existence of the append blob
    await blob_client.create_append_blob()
    
    # Initiate the stream upload process
    await stream_upload(req, blob_client)

    return web.Response(text="CSV file Uploaded")