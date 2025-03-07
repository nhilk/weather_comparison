import logging
import socketio
import logging
import asyncio
import polars as pl

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
        # Set the data to be returned
        sio.data = data

    await sio.connect(url, transports=["websocket"])
    while not event_received:
        await sio.sleep(1)
    await sio.disconnect()
    return pl.DataFrame(sio.data)

def transform_data_facts(ambient_data, location_id: int) -> pl.DataFrame:
    '''
        Transform the data into a format that can be used to create the fact_weather table.
    '''
    if ambient_data is None:
        raise ValueError("Data is required")
    try:
        # Extract the relevant data from the JSON response
        df = pl.DataFrame({
            'date': ambient_data['date'],
            'source': 'ambient_weather',
            'location_id': location_id,
            'temperature': ambient_data['tempf'],
            'pressure': ambient_data['baromrelin'],
            'humidity': ambient_data['humidity'],
            'wind_direction': ambient_data['winddir'],
            'wind_speed': ambient_data['windspdmph_avg10m'],
            'wind_gust': ambient_data['windgustmph'],
            'daily_precipitation': ambient_data['dailyrainin']
        })
        return df
    except Exception as e:
        raise ValueError(f"Error transforming data: {e}")
