from api.ambient_weather_api import get_weather_station_data
import toml
import logging
from database import DB
import polars as pl
import asyncio

logging.basicConfig(
    filename='weather_comparison_ambient_main.log',  # Specify the log file name
    level=logging.DEBUG,  # Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
    filemode='w',  # Mode: 'w' to overwrite, 'a' to append
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s' # Define the log message format
)

def get_config():
    config = toml.load('weather_comp/config.toml')
    return config

async def main():
    logger = logging.getLogger(__name__)
    config = get_config()
    database = DB(logger, config)
    data = await get_weather_station_data(config)
    database.write_to_api_table('ambient_weather', data)
    location_data = {
        'city': config['city'],
        'state': config['state'],
        'country': config['country'],
        'latitude': config['lat'],
        'longitude': config['long']
    }
    #location_id = database.write_to_dim_location(location_data)
    #transformed_data = transform_data_facts(data, location_id)
    #database.write_to_fact_weather(transformed_data)
if __name__ == "__main__":
    asyncio.run(main())