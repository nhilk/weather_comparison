import requests

def get_nsw(lat, long):
    if lat is None or long is None:
        raise ValueError("Latitude and Longitude are required")
    url = f"https://api.weather.gov/points/{lat},{long}"

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data['properties']['forecast'] = 'https://api.weather.gov/gridpoints/forecast'
        return data
    else:
        None

if __name__ == "__main__":
    get_nsw(26.16123,-81.80686)