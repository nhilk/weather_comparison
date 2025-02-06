from sqlalchemy import Column, Integer, String, JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ApiData(Base):
    '''
        Database model for storing data directly from an API.
    '''
    __tablename__ = 'api_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String, nullable=False)
    data = Column(JSON, nullable=False)

class WeatherData(Base):
    '''
        Database model for storing weather data facts gathered from the api_data table.
    '''
    __tablename__ = 'weather_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String, nullable=False)

def init_db(engine):
    Base.metadata.create_all(bind=engine)