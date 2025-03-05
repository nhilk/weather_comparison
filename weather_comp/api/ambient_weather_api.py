import logging
import socketio
import logging
import asyncio

logger = logging.getLogger(__name__)

async def get_weather_station_data(config):
    config = config['ambient_weather_api']
    global event_received
    event_received = False

    url = f"https://rt2.ambientweather.net/?api=1&applicationKey={config['app_key']}"
    sio =  socketio.AsyncClient()
    @sio.event
    async def connect():
        await sio.emit("subscribe", {"apiKeys": [config["api_key"]]})
        print("Connected to Ambient Weather API")

    @sio.event
    async def disconnect():
        print("Disconnected from Ambient Weather API")

    @sio.event
    async def data(data):
        global event_received
        event_received = True
        print(data)
        return data

    await sio.connect(url, transports=["websocket"])
    while not event_received:
        print(event_received)
        await sio.sleep(1)
    await sio.disconnect()
    print("Disconnected from Ambient Weather API")

# def transform_data_facts(ambient_data, location_id):
#     '''
#         Transform the data into a format that can be used to create the fact_weather table.
#     '''
#     if ambient_data is None:
#         raise ValueError("Data is required")
#     try:
#         # Extract the relevant data from the JSON response
#         df = pl.DataFrame({
#             'date': date,
#             'location_id': location_id,
#             'temperature': temperature,
#             'pressure': pressure,
#             'cloud_cover': cloud_cover
#         })
#         return df
#     except Exception as e:
#         raise ValueError(f"Error transforming data: {e}")
