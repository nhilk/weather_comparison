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
        """
        Initializes the database connection and sets up table mappings.

        Args:
            config (dict): Configuration dictionary containing database connection details.
        """
        self.logger = logging.getLogger(__name__)
        self.logger.debug("connecting to the database")
        self.config = config
        self.engine = create_engine(
            self.config["database"]["db_url"],
            echo=True,
            pool_size=10,
            max_overflow=20,
        )
        init_db(self.engine)
        self.tables = {
            "api_data": ApiData,
            "fact_weather": FactWeather,
            "dim_location": DimLocation,
            "source_comparison": SourceComparison,
        }

    def check_columns_match(self, data: pl.DataFrame, table: object) -> bool:
        """
        Checks if column in the provided Polars DataFrame match column in specified table.

        Args:
            data (pl.DataFrame): Polars DataFrame to validate.
            table (object): The SQLAlchemy ORM table object to compare against.

        Returns:
            bool: True if the columns match, False otherwise.

        Raises:
            TypeError: If the provided table is not a valid table type.
        """
        if table not in self.tables.values():
            raise TypeError(f"{type(table)} is not a valid table type")
        if not isinstance(table, (ApiData, FactWeather, DimLocation)):
            raise TypeError(f"{type(table)} is not a valid type for table")
        table_columns = set(table.__table__.columns.keys())
        data_columns = set(data.columns)
        return data_columns.issubset(table_columns)

    def write_to_api_table(
        self, source: str, json_data: dict, date: datetime = None
    ) -> None:
        """
        Writes API data to the ApiData table.

        Args:
            source (str): The source of the API data.
            json_data (dict): The JSON data to write.
            date (datetime, optional): The date of the data. Defaults to the current date and time.

        Raises:
            TypeError: If the provided json_data is not a dictionary.
        """
        if not isinstance(json_data, dict):
            error_message = (
                f"type: {type(json_data)} not of {dict} cannot write to api table"
            )
            self.logger.error(error_message)
            raise TypeError(error_message)
        with Session(self.engine) as session, session.begin():
            if date is not None:
                api_data_entry = ApiData(source=source, date=date, data=json_data)
            else:
                api_data_entry = ApiData(
                    source=source, date=datetime.now(), data=json_data
                )
            session.add(api_data_entry)

    def write_to_fact_weather(self, data: pl.DataFrame) -> None:
        """
        Writes weather data to the FactWeather table.

        Args:
            data (pl.DataFrame): The Polars DataFrame containing weather data.

        Raises:
            TypeError: If the provided data is not a Polars DataFrame.
            Exception: If there is an error during the database write operation.
        """
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
        """
        Reads data from a specified table or executes a custom SQLAlchemy select statement.

        Args:
            select_statement (select, optional): A custom SQLAlchemy select statement. Defaults to None.
            table (str, optional): The name of the table to read from. Defaults to None.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the retrieved data.

        Raises:
            ValueError: If the specified table is not valid.
        """
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
                    data.append(
                        {
                            col: getattr(row[0], col)
                            for col in row[0].__table__.columns.keys()
                        }
                    )
                table_entries = data
            return pl.DataFrame(table_entries)

    def check_existing_location(self, lat: float, long: float) -> int:
        """
        Checks if a location with the specified latitude and longitude exists in the DimLocation table.

        Args:
            lat (float): The latitude of the location.
            long (float): The longitude of the location.

        Returns:
            int: The ID of the location if it exists, None otherwise.

        Raises:
            TypeError: If the latitude or longitude is not a float.
        """
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
        """
        Writes location data to the DimLocation table.

        Args:
            location_frame (pl.DataFrame): The Polars DataFrame containing location data.

        Returns:
            int: The ID of the location in the database.

        Raises:
            ValueError: If the provided data is not a Polars DataFrame or if there is an error during the write operation.
        """
        if not isinstance(location_frame, pl.DataFrame):
            self.logger.error(
                f"type: {type(location_frame)} not of {pl.DataFrame} cannot write to DimLocation table"
            )
            raise ValueError(
                f"type: {type(location_frame)} not of {pl.DataFrame} cannot write to DimLocation table"
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
        """
        Writes data to the SourceComparison table.

        Args:
            data (pl.DataFrame): The Polars DataFrame containing the data to write.

        Raises:
            TypeError: If the provided data is not a Polars DataFrame.
            Exception: If there is an error during the database write operation.
        """
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
