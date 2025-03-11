import requests
import polars as pl
import logging
import json

logger = logging.getLogger(__name__)

def get_forecast_url(lat:str, long:str)->str:
    if lat is None or long is None:
        raise ValueError("Latitude and Longitude are required")
    url = f"https://api.weather.gov/points/{lat},{long}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        combined_endpoint = data['properties']['forecastHourly'] +"?units=us"
        return combined_endpoint
    else:
        None

def get_nws_forecast(lat:str, long:str)->dict:
    url = get_forecast_url(lat, long)
    if url is None:
        raise ValueError("Cannot get forecast url is not valid")
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data['source'] = "https://api.weather.gov"
        return data
    else:
        raise ValueError("Unable to get weather forecast")

def transform_data_facts(nws_data: dict, location_id: int) -> pl.DataFrame:
    '''
        Transform the data into a format that can be used to create the fact_weather table.
    '''
    if nws_data is None:
        raise ValueError("Data is required")
    try:
        # Extract the relevant data from the JSON response
        periods = nws_data['properties'].get('periods',[])
        if periods is None:
            raise ValueError("No periods found in the data")
        # Extract the relevant data from the periods
        temperature = [period['temperature'] for period in periods]
        pressure = None
        cloud_cover = None
        date = [period['startTime'] for period in periods]

        # Create a DataFrame using Polars
        df = pl.DataFrame({
            'date': date,
            'source': nws_data['source'],
            'location_id': location_id,
            'temperature': temperature,
            'pressure': pressure,
            'cloud_cover': cloud_cover
        })
        return df
    except Exception as e:
        raise ValueError(f"Error transforming data: {e}")
    

