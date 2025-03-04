from sqlalchemy.orm import Session
from sqlalchemy import create_engine, select
from database.models import ApiData, init_db, fact_weather, dim_location

import polars as pl
import toml
import logging

class DB:
    def __init__(self, logger, config):
        self.logger = logging.getLogger(__name__)
        self.logger.debug('connecting to the database')
        self.config = config
        self.engine = create_engine(self.config['database']['db_url'], echo=True)
        init_db(self.engine)
        
    def write_to_api_table(self, source, json_data):
        if json_data is None:
            self.logger("No data to write to the database")
            raise ValueError("No data to write to the database")

        with Session(self.engine) as session, session.begin():
            api_data_entry = ApiData(source=source, data=json_data)
            session.add(api_data_entry)

    def read_from_db(self):
        with Session(self.engine) as session, session.begin():
            api_data_entries = session.query(ApiData).all()
        # Print the results
        for entry in api_data_entries:
            print(f"ID: {entry.id}, Source: {entry.source}, Data: {entry.data}, Timestamp: {entry.timestamp}")

    def write_to_fact_weather(self, data: pl.DataFrame):
        if data is None:
            self.logger.error("No data to write to the database")
            raise ValueError("No data to write to the database")
        
        with Session(self.engine) as session, session.begin():
            try:
                data.write_database(table_name='fact_weather', connection = session, if_table_exists='append')
            except Exception as e:
                self.logger.error(f"Error writing to fact_weather table: {e}")
                raise ValueError("Error writing to fact_weather table")

    def write_to_dim_location(self, data):
        if data is None:
            self.logger.error("No data to write to the database")
            raise ValueError("No data to write to the database")
        
        with Session(self.engine) as session, session.begin():         
            # Check if the location already exists
            try:
                existing_location = session.scalars(select(dim_location).filter_by(city=data['city'], state=data['state'], country=data['country']))
            except Exception as e:
                self.logger.error(f"Error checking for existing location: {e}")
                raise ValueError("Error checking for existing location")
            if existing_location:
                location_id = session.scalars(select(dim_location.id).filter_by(city=data['city'], state=data['state'], country=data['country'])).first
                self.logger.info(F"Location already exists in the database with id {location_id}")
                return location_id
            else:
                try:
                    data.write_database(table_name='dim_location', connection = session, if_table_exists='append')
                    location_id = session.scalars(select(dim_location.id).filter_by(city=data['city'], state=data['state'], country=data['country'])).first
                    self.logger.info(f"Location added to the database with id {location_id}")
                    return location_id
                except Exception as e:
                    self.logger.error(f"Error writing to dim_location table: {e}")
                    raise ValueError("Error writing to dim_location table")

