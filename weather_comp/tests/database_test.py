import pytest
from unittest.mock import patch, Mock
import os
import sys
sys.path.append(os.path.abspath(''))
from database.db import DB
from database.models import ApiData, fact_weather, dim_location
import polars as pl
import datetime

@pytest.fixture(scope='module')
def mock_config():
    return {'database':{'db_url': 'postgresql://hilkshake:SomethingIsWrong@0.0.0.0:5432/test_weather_db'}}
@pytest.fixture
def time():
    return datetime.datetime.now()

@pytest.fixture
def mock_data(time):
    
    return {
        'properties': {
            'forecastHourly':'https://api.weather.gov/gridpoints/MFL/52,97/forecast',
            'periods': [
                {
                    'temperature': 75,
                    'startTime': time.isoformat()
                },
                {
                    'temperature': 76,
                    'startTime': time.isoformat()
                }
            ]
        }
    }

@pytest.fixture(scope='module')
@patch('database.db.logging')
def test_db(mock_logger, mock_config):
    return DB(mock_logger, mock_config)

def test_write_to_api_table_negative(test_db):
    with pytest.raises(TypeError) as context:
        test_db.write_to_api_table('https://api.weather.gov', 'This is not a dictionary')
    assert "No/incorrect data to write to the database" in str(context.value)

def test_write_to_api_table(test_db, time, mock_data):
    test_db.write_to_api_table('https://api.weather.gov', mock_data, time)
    data = test_db.read_from_api_table((ApiData.date == time))
    df = pl.DataFrame(data)
    assert df.shape[0] == 1

def test_write_to_dim_location(test_db):
    loc_id = test_db.write_to_dim_location({'city': 'test', 'state':'test_state', 'country':'test_country', 'latitude': 26.16123, 'longitude': -81.80686})
    print(f'loc_id = {loc_id}')
    data = test_db.read_from_dim_location(dim_location.latitude == 26.16123)
    df = pl.DataFrame(data)
    print(f'data = {data}, df = {df}')
    assert df.shape[0] == 1


def test_write_to_fact_weather(test_db, time,mock_data):
    df = pl.DataFrame({
        'date': [time.isoformat(), time.isoformat()],
        'location_id': 1,
        'temperature': [75, 76],
        'pressure': [None, None],
        'cloud_cover': [None, None]
    })
    test_db.write_to_fact_weather(df)
    df = pl.DataFrame(test_db.read_from_fact_weather(fact_weather.date == time))
    assert df.shape[0] == 2
    