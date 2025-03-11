import pytest
from unittest.mock import patch, Mock
import os
import sys

sys.path.append(os.path.abspath(""))
from database.db import DB
from database.models import ApiData, fact_weather, dim_location
import polars as pl
import datetime
import logging

logging.basicConfig(
    filename="logs/weather_comparison_db_testing.log",  # Specify the log file name
    level=logging.DEBUG,  # Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
    filemode="w",  # Mode: 'w' to overwrite, 'a' to append
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # Define the log message format
)


@pytest.fixture(scope="module")
def mock_config():
    return {
        "database": {
            "db_url": "postgresql://hilkshake:SomethingIsWrong@0.0.0.0:5432/test_weather_db"
        }
    }


@pytest.fixture
def time():
    return datetime.datetime.now()


@pytest.fixture
def mock_data(time):
    return        {
            "properties": {
                "forecastHourly": "https://api.weather.gov/gridpoints/MFL/52,97/forecast",
                "periods": [
                    {"temperature": 75, "startTime": time.isoformat()},
                    {"temperature": 76, "startTime": time.isoformat()},
                ],
            }
        }


@pytest.fixture
def mock_location_data():
    return pl.DataFrame(
        {
            "city": "test",
            "state": "test_state",
            "country": "test_country",
            "latitude": 10.12121,
            "longitude": -90.22222,
        }
    )


@pytest.fixture(scope="module")
def test_db(mock_config):
    return DB(mock_config)


def test_write_to_api_table_negative(test_db):
    with pytest.raises(TypeError) as context:
        test_db.write_to_api_table(
            "https://api.weather.gov", "This is not a dictionary"
        )
    assert "cannont write to api table" in str(context.value)


def test_write_to_api_table(test_db, time, mock_data):
    test_db.write_to_api_table("https://api.weather.gov", mock_data, time)
    data = test_db.read_from_api_table((ApiData.date == time))
    df = pl.DataFrame(data)
    assert df.shape[0] == 1


def test_check_existing_location_negative(test_db):
    lat, long = None, None
    with pytest.raises(TypeError) as context:
        test_db.check_existing_location(lat, long)
    assert "are not valid types for latitude and longitude" in str(context.value)


def test_write_to_dim_location(test_db, mock_location_data):
    loc_id = test_db.write_to_dim_location(mock_location_data)
    print(f"loc_id = {loc_id}")
    data = test_db.read_from_dim_location(dim_location.latitude == 10.12121)
    df = pl.DataFrame(data)
    print(f"data = {data}, df = {df}")
    assert df.shape[0] == 1


def test_check_existing_location(test_db, mock_location_data):
    loc_id = test_db.write_to_dim_location(mock_location_data)
    assert test_db.check_existing_location(10.12121, -90.22222) == loc_id


def test_write_to_fact_weather(test_db, time, mock_data):
    df = pl.DataFrame(
        {
            "date": [time.isoformat(), time.isoformat()],
            "location_id": 1,
            'source': 'https://api.weather.gov',
            "temperature": [75, 76],
            "pressure": [None, None],
            "cloud_cover": [None, None],
        }
    )
    test_db.write_to_fact_weather(df)
    df = pl.DataFrame(test_db.read_from_fact_weather(fact_weather.date == time))
    assert df.shape[0] == 2
