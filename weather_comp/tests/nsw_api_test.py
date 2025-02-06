import unittest
from unittest.mock import patch
from weather_comp.api import get_nsw

class TestGetNsw(unittest.TestCase):

    @patch('weather_comp.api.nws_api.requests.get')
    def test_get_forcast_url_valid(self, mock_get):
        mock_response = unittest.mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'properties':{'forecastHourly':'https://api.weather.gov/gridpoints/MFL/52,97/forecast'}}
        mock_get.return_value = mock_response

        result = get_nsw(26.16123, -81.80686)
        self.assertEqual(result['properties'], 'https://api.weather.gov/gridpoints/MFL/52,97/forecast')

    @patch('weather_comp.api.nws_api.requests.get')
    def test_get_forcast_url_missing_lat(self, mock_get):
        with self.assertRaises(ValueError) as context:
            get_nsw(None, -81.80686)
        self.assertEqual(str(context.exception), "Latitude and Longitude are required")

    @patch('weather_comp.api.nws_api.requests.get')
    def test_get_forcast_url_missing_long(self, mock_get):
        with self.assertRaises(ValueError) as context:
            get_nsw(26.16123, None)
        self.assertEqual(str(context.exception), "Latitude and Longitude are required")

    @patch('weather_comp.api.nws_api.requests.get')
    def test_get_forcast_url_non_200_response(self, mock_get):
        mock_response = unittest.mock.Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        result = get_nsw(26.16123, -81.80686)
        self.assertIsNone(result)

    @patch('weather_comp.api.nws_api.requests.get')
    def test_get_weather_forcast_valid(self, mock_get):
        mock_response = unittest.mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'properties':{'forecastHourly':'https://api.weather.gov/gridpoints/MFL/52,97/forecast'}}
        mock_get.return_value = mock_response

        result = get_nsw(26.16123, -81.80686)
        self.assertEqual(result['properties'], 'https://api.weather.gov/gridpoints/MFL/52,97/forecast')

    @patch('weather_comp.api.nws_api.requests.get')
    def test_get_weather_forcast_non_200_response(self, mock_get):
        mock_response = unittest.mock.Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        result = get_nsw(26.16123, -81.80686)
        self.assertIsNone(result)
        
if __name__ == '__main__':
    unittest.main()