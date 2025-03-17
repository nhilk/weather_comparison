from sqlalchemy.orm import Session
from sqlalchemy import create_engine, and_
from sqlalchemy import select
from database.models import (
    ApiData,
    init_db,
    FactWeather,
    DimLocation,
    SourceComparison,
)
from datetime import datetime
import polars as pl
import logging


class DB:
    
    
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.logger.debug("connecting to the database")
        self.config = config
        self.engine = create_engine(self.config["database"]["db_url"], echo=True)
        init_db(self.engine)
        self.tables = {
            "api_data": ApiData,
            "fact_weather": FactWeather,
            "dim_location": DimLocation,
            "source_comparison": SourceComparison,
        }

    def check_columns_match(self, data: pl.DataFrame, table: object) -> bool:
        if not isinstance(table, [ApiData, FactWeather, DimLocation]):
            raise TypeError(f"{type(table)} is not a valid type for table")
        table_columns = set(table.__table__.columns.keys())
        data_columns = set(data.columns)
        if data_columns.issubset(table_columns):
            return True
        else:
            return False

    def write_to_api_table(
        self, source: str, json_data: dict, date: datetime = None
    ) -> None:
        if not isinstance(json_data, dict):
            self.logger.error(
                f"type: {type(json_data)} not of {dict} cannont write to api table"
            )
            raise TypeError(
                f"type: {type(json_data)} not of {dict} cannont write to api table"
            )
        with Session(self.engine) as session, session.begin():
            if date is not None:
                api_data_entry = ApiData(source=source, date=date, data=json_data)
            else:
                api_data_entry = ApiData(
                    source=source, date=datetime.now(), data=json_data
                )
            session.add(api_data_entry)

    def write_to_fact_weather(self, data: pl.DataFrame) -> None:
        if not isinstance(data, pl.DataFrame):
            raise TypeError(
                f"{type(data)} is not a valid type for writing to the database"
            )
        with Session(self.engine) as session, session.begin():
            try:
                data.write_database(
                    table_name=str(
                        FactWeather.__table__.schema + "." + FactWeather.__tablename__
                    ),
                    connection=session,
                    if_table_exists="append",
                )
                self.logger.info("Data written to FactWeather table")
            except Exception as e:
                self.logger.error(f"Error writing to FactWeather table: {e}")
                raise e

    def read_from_table(
        self, select_statement: select = None, table: str = None
    ) -> pl.DataFrame:
        if (table not in self.tables.keys()) & (table is not None):
            raise ValueError(
                f"{table} is not a valid table. Please choose from {self.tables.values()}"
            )
        with Session(self.engine) as session, session.begin():
            if select_statement is not None:
                table_entries = session.execute(select_statement).all()
            elif select_statement is None:
                table_entries = session.execute(select(self.tables[table]))
                data = []
                for row in table_entries:
                    data.append({col: getattr(row[0], col) for col in row[0].__table__.columns.keys()})
                print(data)
                table_entries = data
            return pl.DataFrame(table_entries)

    def check_existing_location(self, lat: float, long: float) -> int:
        if not isinstance(lat, float) or not isinstance(long, float):
            raise TypeError(
                f"{type(lat)} and {type(long)} are not valid types for latitude and longitude"
            )
        with Session(self.engine) as session, session.begin():
            location_id = (
                session.query(DimLocation)
                .filter(
                    and_(DimLocation.latitude == lat, DimLocation.longitude == long)
                )
                .all()
            )
            if len(location_id) > 0:
                return location_id[0].id
            else:
                return None

    def write_to_DimLocation(self, location_frame: pl.DataFrame) -> int:
        if not isinstance(location_frame, pl.DataFrame):
            self.logger.error(
                f"type: {type(location_frame)} not of {pl.DataFrame} cannont write to api table"
            )
            raise ValueError(
                f"type: {type(location_frame)} not of {pl.DataFrame} cannont write to api table"
            )

        with Session(self.engine) as session, session.begin():
            location_id = self.check_existing_location(
                location_frame["latitude"][0], location_frame["longitude"][0]
            )
            if location_id is not None:
                self.logger.info(
                    f"Location already exists in the database with id {location_id}"
                )
                return location_id
            else:
                try:
                    location_frame.write_database(
                        table_name=str(
                            DimLocation.__table__.schema
                            + "."
                            + DimLocation.__tablename__
                        ),
                        connection=session,
                        if_table_exists="append",
                    )
                    location_id = (
                        session.query(DimLocation)
                        .filter(
                            and_(
                                DimLocation.latitude == location_frame["latitude"][0],
                                DimLocation.longitude == location_frame["longitude"][0],
                            )
                        )
                        .all()
                    )
                    self.logger.info(
                        f"Location added to the database with id {location_id}"
                    )
                    return location_id[0]
                except Exception as e:
                    self.logger.error(f"Error writing to DimLocation table: {e}")
                    raise ValueError("Error writing to DimLocation table")

    def write_to_source_comparison(self, data: pl.DataFrame) -> None:
        if not isinstance(data, pl.DataFrame):
            raise TypeError(
                f"{type(data)} is not a valid type for writing to the database"
            )
        with Session(self.engine) as session, session.begin():
            try:
                data.write_database(
                    table_name=str(
                        SourceComparison.__table__.schema
                        + "."
                        + SourceComparison.__tablename__
                    ),
                    connection=session,
                    if_table_exists="append",
                )
                self.logger.info("Data written to SourceComparison table")
            except Exception as e:
                self.logger.error(f"Error writing to SourceComparison table: {e}")
                raise e
