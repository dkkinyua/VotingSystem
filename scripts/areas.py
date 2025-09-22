import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DB_URL = os.getenv("DB_URL")
areas = {
    "name": [
        "Lagos",
        "Cairo",
        "Kinshasa",
        "Johannesburg",
        "Nairobi",
        "Addis Ababa",
        "Dar es Salaam",
        "Casablanca",
        "Abidjan",
        "Algiers"
    ],
    "latitude": [
        6.5244,
        30.0444,
        -4.4419,
        -26.2041,
        -1.2921,
        9.0192,
        -6.7924,
        33.5731,
        5.3453,
        36.7538
    ],
    "longitude": [
        3.3792,
        31.2357,
        15.2663,
        28.0473,
        36.8219,
        38.7525,
        39.2083,
        -7.5898,
        -4.0244,
        3.0588
    ]
}

def main(areas):
    df = pd.DataFrame(areas)
    try:
        engine = create_engine(DB_URL)
        df.to_sql(name='areas', con=engine, schema='votingsystem', index=False, if_exists='append')
        print('Data loaded into areas successfully!')
    except Exception as e:
        print(f"Error loading data into Postgres: {e}")

if __name__ == '__main__':
    main(areas)