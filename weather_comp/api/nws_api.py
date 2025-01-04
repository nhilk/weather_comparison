import requests

def get_forcast_url(lat, long):
    if lat is None or long is None:
        raise ValueError("Latitude and Longitude are required")
    url = f"https://api.weather.gov/points/{lat},{long}"

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['properties']['forecastHourly']
    else:
        None

def get_weather_forcast(url):
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
        return None
    return get_weather_forcast(forcast_url)

if __name__ == "__main__":
    print(get_nsw(26.16123,-81.80686))