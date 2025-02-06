from api.nws_api import get_nsw
import toml
import logging
from database import DB

logging.basicConfig(
    filename='weather_comparison.log',  # Specify the log file name
    level=logging.DEBUG,  # Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
    filemode='w',  # Mode: 'w' to overwrite, 'a' to append
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s' # Define the log message format
)

def get_config():
    config = toml.load('weather_comp/config.toml')
    return config

def main():
    logger = logging.getLogger(__name__)
    config = get_config()
    database = DB(logger)
    data = get_nsw(config['lat'],config['long'])
    database.write_to_api_table('nsw', data)
    # with open('/home/hilkshake/documents/weather_comparison/weather_comp/nsw_data.json', 'w') as json_file:
    #     json.dump(data, json_file)

if __name__ == "__main__":
    main()