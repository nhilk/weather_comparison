import logging
import socketio
from database.db import DB
from database.models import ApiData
import logging

logger = logging.getLogger(__name__)

async def get_weather_station_data(config):
    logger = logging.getLogger(__name__)
    config = config['ambient_weather_api']
    database = DB(logger)
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
        print("Received data from Ambient Weather API")
        print(data)
        await database.write_to_api_table("dads_ambient_weather", data)

    await sio.connect(url, transports=["websocket"])
    while not event_received:
        await sio.sleep(1)
    await sio.disconnect()
    await database.close()
    print("Disconnected from Ambient Weather API")
    
