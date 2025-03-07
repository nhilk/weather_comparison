from sqlalchemy.orm import Session
from sqlalchemy import create_engine, and_
from sqlalchemy.sql.elements import BooleanClauseList, BinaryExpression
from database.models import ApiData, init_db, fact_weather, dim_location
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

    def check_valid_filter_conditions(self, filter_conditions: BooleanClauseList|BinaryExpression):
        """_summary_

        Args:
            filter_conditions (_type_): _description_

        Raises:
            TypeError: _description_
        """
        if not isinstance(filter_conditions, (BooleanClauseList, BinaryExpression)):
            raise TypeError(
                f"{type(filter_conditions)} is not a valid type for filter_conditions"
            )

    def check_columns_match(self, data: pl.DataFrame, table: object) -> bool:
        if not isinstance(table, [ApiData, fact_weather, dim_location]):
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
        print(f' write_to_api_table json type{json_data}')
        if not isinstance(json_data, dict):
            self.logger.error(f"type: {type(json_data)} not of {pl.DataFrame} cannont write to api table")
            raise TypeError(
                f"type: {type(json_data)} not of {pl.DataFrame} cannont write to api table"
            )
        json_data = json_data.to_dict(as_series=False)
        with Session(self.engine) as session, session.begin():
            if date is not None:
                api_data_entry = ApiData(source=source, date=date, data=json_data)
            else:
                api_data_entry = ApiData(
                    source=source, date=datetime.now(), data=json_data
                )
            session.add(api_data_entry)

    def read_from_api_table(
        self, filter_conditions: BooleanClauseList | BinaryExpression = None
    ) -> pl.DataFrame:
        with Session(self.engine) as session, session.begin():
            if filter_conditions is not None:
                self.check_valid_filter_conditions(filter_conditions)
                api_data_entries = (
                    session.query(ApiData).filter(filter_conditions).all()
                )
            elif filter_conditions is None:
                api_data_entries = session.query(ApiData).all()

        return pl.DataFrame(api_data_entries)

    def write_to_fact_weather(self, data: pl.DataFrame) -> None:
        if not isinstance(data, pl.DataFrame):
            raise TypeError(
                f"{type(data)} is not a valid type for writing to the database"
            )
        with Session(self.engine) as session, session.begin():
            try:
                data.write_database(
                    table_name=str(
                        fact_weather.__table__.schema + "." + fact_weather.__tablename__
                    ),
                    connection=session,
                    if_table_exists="append",
                )
                self.logger.info("Data written to fact_weather table")
            except Exception as e:
                self.logger.error(f"Error writing to fact_weather table: {e}")
                raise e

    def read_from_fact_weather(
        self, filter_conditions: BooleanClauseList | BinaryExpression = None
    ) -> pl.DataFrame:
        with Session(self.engine) as session, session.begin():
            if filter_conditions is not None:
                self.check_valid_filter_conditions(filter_conditions)
                fact_weather_entries = (
                    session.query(fact_weather).filter(filter_conditions).all()
                )
                return pl.DataFrame(fact_weather_entries)
            elif filter_conditions is None:
                fact_weather_entries = session.query(fact_weather).all()
                return pl.DataFrame(fact_weather_entries)

    def check_existing_location(self, lat: float, long: float) -> int:
        if not isinstance(lat, float) or not isinstance(long, float):
            raise TypeError(
                f"{type(lat)} and {type(long)} are not valid types for latitude and longitude"
            )
        with Session(self.engine) as session, session.begin():
            location_id = (
                session.query(dim_location)
                .filter(
                    and_(dim_location.latitude == lat, dim_location.longitude == long)
                )
                .all()
            )
            if len(location_id) > 0:
                return location_id[0].id
            else:
                return None

    def write_to_dim_location(self, location_frame: pl.DataFrame) -> int:
        if not isinstance(location_frame, pl.DataFrame):
            self.logger.error(f"type: {type(location_frame)} not of {pl.DataFrame} cannont write to api table")
            raise ValueError(f"type: {type(location_frame)} not of {pl.DataFrame} cannont write to api table")

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
                            dim_location.__table__.schema
                            + "."
                            + dim_location.__tablename__
                        ),
                        connection=session,
                        if_table_exists="append",
                    )
                    location_id = (
                        session.query(dim_location)
                        .filter(
                            and_(
                                dim_location.latitude == location_frame["latitude"][0],
                                dim_location.longitude == location_frame["longitude"][0],
                            )
                        )
                        .all()
                    )
                    self.logger.info(
                        f"Location added to the database with id {location_id}"
                    )
                    return location_id[0]
                except Exception as e:
                    self.logger.error(f"Error writing to dim_location table: {e}")
                    raise ValueError("Error writing to dim_location table")

    def read_from_dim_location(
        self, filter_conditions: BooleanClauseList | BinaryExpression = None
    ) -> pl.DataFrame:
        with Session(self.engine) as session, session.begin():
            if filter_conditions is not None:
                self.check_valid_filter_conditions(filter_conditions)
                dim_location_entries = (
                    session.query(dim_location).filter(filter_conditions).all()
                )
                return pl.DataFrame(dim_location_entries)
            elif filter_conditions is None:
                dim_location_entries = session.query(dim_location).all()
                return pl.DataFrame(dim_location_entries)
