import aiohttp
import asyncio

async def stream_file_to_server(file_path, url):
    async with aiohttp.ClientSession() as session:
        await session.post(url, data=stream_generator(file_path))
            # print('Status:', response.status)
            # print('Content-type:', response.headers.get('content-type'))
            # Content-length might not be available for streamed uploads
            # print('Body:', await response.text())

async def stream_generator(file_path):
    # Stream the file directly without loading it entirely into memory
    chunk_size = 8 * 1024 * 1024  # Define your own chunk size
    with open(file_path, 'rb') as file:
        while chunk := file.read(chunk_size):
            yield chunk
            print(f"Sent chunk: {len(chunk)} bytes")

# URL and file path
url = 'http://localhost:5000/api/h1'
file_path = r'D:\Functions\proxy_blob\funclogs.csv'

# Run the streaming upload
asyncio.run(stream_file_to_server(file_path, url))
