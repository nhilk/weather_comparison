import toml
import logging
import polars as pl
from database.db import DB
from api.nws_api import get_nws_forecast, transform_data_facts

logging.basicConfig(
    filename='logs/weather_comparison_nws_main.log',
    level=logging.ERROR,  # Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
    filemode='w',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_config():
    """
    Loads the configuration from the 'config.toml' file.
    """
    return toml.load('config.toml')

def main():
    """
    Main function to fetch weather data, transform it, and store it in the database.
    """
    logger = logging.getLogger(__name__)
    config = get_config()
    database = DB(config)

    # Write location data to the database
    location_data = pl.DataFrame({
        'city': config['city'],
        'state': config['state'],
        'country': config['country'],
        'latitude': config['lat'],
        'longitude': config['long']
    })
    location_id = database.write_to_DimLocation(location_data)

    # Fetch and process weather data
    data = get_nws_forecast(config['lat'], config['long'])
    database.write_to_api_table(data['source'], data)
    transformed_data = transform_data_facts(data, location_id)
    database.write_to_FactWeather(transformed_data)

if __name__ == "__main__":
    main()
