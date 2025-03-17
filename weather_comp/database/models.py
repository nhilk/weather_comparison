from sqlalchemy import Column, Integer, String, JSON, Float, DateTime, Boolean, MetaData
from sqlalchemy.orm import DeclarativeBase


schema = MetaData(schema="weather_comp")


class Base(DeclarativeBase):
    metadata = schema


class ApiData(Base):
    """
    Database model for storing data directly from an API.
    """

    __tablename__ = "api_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String, nullable=False)
    date = Column(DateTime, nullable=False)
    ingested = Column(Boolean, nullable=True)
    data = Column(JSON, nullable=False)

    def get_columns():
        return({c.name: c for c in ApiData.__table__.columns})


class FactWeather(Base):
    """
    Database model for storing weather data facts gathered from the api_data table.
    """

    __tablename__ = "fact_weather"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, nullable=False)
    source = Column(String, nullable=False)
    location_id = Column(Integer, nullable=False)
    temperature = Column(Float, nullable=True)
    pressure = Column(Float, nullable=True)
    cloud_cover = Column(Float, nullable=True)
    humidity = Column(Integer, nullable=True)
    wind_direction = Column(Integer, nullable=True)
    wind_speed = Column(Float, nullable=True)
    wind_gust = Column(Float, nullable=True)
    daily_precipitation = Column(Float, nullable=True)

    def get_columns():
        return({c.name: c for c in FactWeather.__table__.columns})


class SourceComparison(Base):
    """
    Database model for storing comparison data between two sources.
    """

    __tablename__ = "source_comparison"
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, nullable=False)
    source1 = Column(String, nullable=False)
    source2 = Column(String, nullable=False)
    date1 = Column(DateTime, nullable=False)
    date2 = Column(DateTime, nullable=False)
    temperature = Column(Float, nullable=True)
    pressure = Column(Float, nullable=True)
    cloud_cover = Column(Float, nullable=True)
    humidity = Column(Integer, nullable=True)
    wind_direction = Column(Integer, nullable=True)
    wind_speed = Column(Float, nullable=True)
    wind_gust = Column(Float, nullable=True)
    daily_precipitation = Column(Float, nullable=True)
    
    def get_columns():
        return({c.name: c for c in SourceComparison.__table__.columns})


class DimLocation(Base):
    """
    Database model for storing location data.
    """

    __tablename__ = "dim_location"
    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String, nullable=False)
    state = Column(String, nullable=False)
    country = Column(String, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)

    def get_columns():
        return({c.name: c for c in DimLocation.__table__.columns})


def init_db(engine):
    Base.metadata.create_all(bind=engine)
