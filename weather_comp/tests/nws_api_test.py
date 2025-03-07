import pytest
from unittest.mock import patch, Mock
import os
import sys
sys.path.append(os.path.abspath(''))
from api.nws_api import get_nws_forecast, get_forecast_url




@pytest.fixture
def mock_weather_data():
    return {
        'properties': {
            'forecastHourly':'https://api.weather.gov/gridpoints/MFL/52,97/forecast',
            'periods': [
                {
                    'temperature': 75,
                    'startTime': '2021-10-14T12:00:00-04:00'
                },
                {
                    'temperature': 76,
                    'startTime': '2021-10-14T13:00:00-04:00'
                }
            ]
        }
    }
@pytest.fixture
def mock_forecast_url():
    return 'https://api.weather.gov/gridpoints/MFL/52,97/forecast?units=us'

@patch('api.nws_api.logger')
def test_get_forecast_url_valid(mock_logger, mock_forecast_url):
    with patch('api.nws_api.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'properties': {
                'forecastHourly':'https://api.weather.gov/gridpoints/MFL/52,97/forecast'}}
        mock_get.return_value = mock_response
        result = get_forecast_url(10.12121, -90.22222)
    assert result == mock_forecast_url

@patch('api.nws_api.logger')
def test_get_forecast_url_missing_lat(mock_logger):
    with pytest.raises(ValueError) as context:
        get_forecast_url(None, -90.22222)
    assert "Latitude and Longitude are required" in str(context.value)

@patch('api.nws_api.logger')
def test_get_forecast_url_missing_long(mock_logger):
    with pytest.raises(ValueError) as context:
        get_forecast_url(10.12121, None)
    assert "Latitude and Longitude are required" in str(context.value)


@patch('api.nws_api.logger')
def test_get_forecast_url_non_200_response(mock_logger):
    with patch('api.nws_api.requests.get') as mock_get, patch('api.get_forecast_url') as mock_url:
        mock_url.return_value = mock_forecast_url
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        result = get_forecast_url(10.12121, -90.22222)
    assert result is None

@patch('api.nws_api.logger')
def test_get_nsw_forecast_valid(mock_logger, mock_weather_data):
    with patch('api.nws_api.requests.get') as mock_get, patch('api.nws_api.get_forecast_url') as mock_url:
        mock_url.return_value = mock_forecast_url
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_weather_data
        mock_get.return_value = mock_response
        result = get_nws_forecast(10.12121, -90.22222)
    assert result['source'] == "https://api.weather.gov"

@patch('api.nws_api.logger')
@patch('api.nws_api.requests.get')
@patch('api.nws_api.get_forecast_url')
def test_get_nsw_forecast_non_200_response(mock_logger, mock_get, mock_url):
    with pytest.raises(ValueError) as context:
        mock_url.return_value = mock_forecast_url
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        get_nws_forecast(10.12121, -90.22222)
    assert "Unable to get weather forecast" in str(context.value)