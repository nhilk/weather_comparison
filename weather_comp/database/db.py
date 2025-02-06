from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from database.models import ApiData, init_db
import toml

class DB:
    def __init__(self, logger):
        self.load_config()
        self.engine = create_engine(self.config['db_url'])
        init_db(self.engine)
        
    def load_config(self):
        self.config = toml.load('weather_comp/database/db_config.toml')
        
    def write_to_api_table(self, source, json_data):
        if json_data is None:
            raise ValueError("No data to write to the database")

        with Session(self.engine) as session, session.begin():
            # Create a new ApiData entry
            api_data_entry = ApiData(source=source, data=json_data)
            session.add(api_data_entry)

    def read_from_db(self):
        with Session(self.engine) as session, session.begin():
            api_data_entries = session.query(ApiData).all()
        # Print the results
        for entry in api_data_entries:
            print(f"ID: {entry.id}, Source: {entry.source}, Data: {entry.data}, Timestamp: {entry.timestamp}")