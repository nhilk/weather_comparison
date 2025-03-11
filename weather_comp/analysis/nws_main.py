import toml
import logging
import polars as pl
import sys
sys.path.append('weather_comp')
from database import DB
from api.nws_api import get_nws_forecast, transform_data_facts
import os

logging.basicConfig(
    filename='logs/weather_comparison_nws_main.log',
    level=logging.ERROR,  # Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
    filemode='w',  
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def get_config():
    config = toml.load('weather_comp/config.toml')
    return config

def main():
    logger = logging.getLogger(__name__)
    config = get_config()
    database = DB(config)
    location_data = pl.DataFrame({
        'city': config['city'],
        'state': config['state'],
        'country': config['country'],
        'latitude': config['lat'],
        'longitude': config['long']
    })
    location_id = database.write_to_dim_location(location_data)
    data = get_nws_forecast(config['lat'],config['long'])
    database.write_to_api_table(data['source'], data)
    transformed_data = transform_data_facts(data, location_id)
    database.write_to_fact_weather(transformed_data)
if __name__ == "__main__":
    main()
