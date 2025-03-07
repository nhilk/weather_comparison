from api.nws_api import get_nws_forecast, transform_data_facts
import toml
import logging
from database import DB
import polars as pl

logging.basicConfig(
    filename='weather_comparison_nws_main.log',  # Specify the log file name
    level=logging.ERROR,  # Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
    filemode='w',  # Mode: 'w' to overwrite, 'a' to append
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s' # Define the log message format
)

def get_config():
    config = toml.load('weather_comp/config.toml')
    return config

def main():
    logger = logging.getLogger(__name__)
    config = get_config()
    database = DB(config)
    location_data = {
        'city': config['city'],
        'state': config['state'],
        'country': config['country'],
        'latitude': config['lat'],
        'longitude': config['long']
    }
    location_id = database.write_to_dim_location(location_data)
    data = get_nws_forecast(config['lat'],config['long'])
    database.write_to_api_table('nsw', data)
    transformed_data = transform_data_facts(data, location_id)
    database.write_to_fact_weather(transformed_data)
if __name__ == "__main__":
    main()
