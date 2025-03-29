from database.db import DB
from database.models import FactWeather, SourceComparison
from sqlalchemy import select
import polars as pl
import logging
import toml
from sqlalchemy import func


logging.basicConfig(
    filename="logs/weather_comparison_source_comparison.log",
    level=logging.ERROR,  # Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
    filemode="w",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def get_config():
    config = toml.load("config.toml")
    return config


def main():
    logger = logging.getLogger(__name__)
    config = get_config()
    database = DB(config)
    """
    SELECT s1.source, s2.source, s1.location_id, (s1.temperature - s2.temperature) as temperature
    FROM weather_comp.fact_weather s1, weather_comp.fact_weather s2
    WHERE s1.source = 'https://ambientweather.net'
    AND date_trunc('hour', s1.date) = date_trunc('hour', s2.date)
    AND s1.id <> s2.id
    """
    s1 = FactWeather.__table__.alias("s1")
    s2 = FactWeather.__table__.alias("s2")
    source_comp_statement = select(
        s1.c.source.label("source1"),
        s2.c.source.label("source2"),
        s1.c.location_id,
        s1.c.date.label('date1'),
        s2.c.date.label('date2'),
        (s1.c.temperature - s2.c.temperature).label("temperature"),
    ).where(
        s1.c.source == "https://ambientweather.net",
        func.date_trunc("hour", s1.c.date) == func.date_trunc("hour", s2.c.date),
        s1.c.id != s2.c.id,
    )
    source_comp_frame = database.read_from_table(select_statement=source_comp_statement)
    print(source_comp_frame)
    database.write_to_source_comparison(source_comp_frame)


if __name__ == "__main__":
    main()
