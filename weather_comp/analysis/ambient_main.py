

import toml
import logging
import polars as pl
import asyncio
import json
import sys
sys.path.append('weather_comp')
from api.ambient_weather_api import get_weather_station_data, transform_data_facts
from database import DB

logging.basicConfig(
    filename='logs/weather_comparison_ambient_main.log',  # Specify the log file name
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
    database = DB(config)
    data = await get_weather_station_data(config)
    database.write_to_api_table(data['source'], data)
    location_data = pl.DataFrame({
        'city': config['city'],
        'state': config['state'],
        'country': config['country'],
        'latitude': config['lat'],
        'longitude': config['long']
    })
    location_id = database.write_to_dim_location(location_data)
    transformed_data = transform_data_facts(data, location_id)
    database.write_to_fact_weather(transformed_data)
if __name__ == "__main__":
    asyncio.run(main())