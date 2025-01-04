from sqlalchemy import create_engine, Column, Integer, String, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class ApiData(Base):
    __tablename__ = 'api_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String, nullable=False)
    data = Column(JSON, nullable=False)

# Database connection setup
DATABASE_URL = "postgresql://postgres:mollie@127.0.0.1:8080/your_database"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)