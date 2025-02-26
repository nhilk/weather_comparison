import requests
import polars as pl

def get_forcast_url(lat:str, long:str)->str:
    if lat is None or long is None:
        raise ValueError("Latitude and Longitude are required")
    url = f"https://api.weather.gov/points/{lat},{long}"

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        combined_endpoint = data['properties']['forecast'] +"?units=us"
        print(combined_endpoint)
        return combined_endpoint
    else:
        None

def get_weather_forcast(url:str)->dict:
    if url is None:
        raise ValueError("URL is required")
    response = requests.get(url)
    if response.status_code == 200:
        weather = response.json()
        weather['source'] = "https://api.weather.gov"
        return response.json()
    else:
        raise ValueError("Unable to get weather forcast")

def get_nsw(lat, long):
    forcast_url = get_forcast_url(lat, long)
    if forcast_url is None:
        raise ValueError("Unable to get forecast URL")
    return get_weather_forcast(forcast_url)

def transform_data_facts(data, location_id):
    '''
        Transform the data into a format that can be used to create the fact_weather table.
    '''
    if data is None:
        raise ValueError("Data is required")
    try:
        # Extract the relevant data from the JSON response
        periods = data['properties'].get('periods',[])
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
            'location_id': location_id,
            'temperature': temperature,
            'pressure': pressure,
            'cloud_cover': cloud_cover
        })
        return df
    except Exception as e:
        raise ValueError(f"Error transforming data: {e}")
    