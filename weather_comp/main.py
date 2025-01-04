from api.nws_api import get_nsw

from database.models import SessionLocal, ApiData, init_db

def main():
    init_db()  # Initialize the database

    session = SessionLocal()

    # Get data from APIs
    data1 = get_data_from_api1()
    

    # Store data in the database
   

    # Perform data analysis

    session.close()

if __name__ == "__main__":
    main()