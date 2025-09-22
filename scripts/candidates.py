import os
import psycopg2
from dotenv import load_dotenv
from faker import Faker

load_dotenv()
fake = Faker()
DB_USER = os.getenv('DB_USER')
DB_PWD = os.getenv('DB_PWD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

def main(n):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PWD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()

        for i in range(1, n + 1):
            candidate_id = i
            name = fake.name()
            area_id = i

            cur.execute("""
                INSERT INTO votingsystem.candidates
                (candidate_id, name, area_id)
                VALUES (%s, %s, %s)
            """, (candidate_id, name, area_id))
            
        print("Data loaded into votingsystem.candidates successfully!")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error connecting psycopg2: {e}")

if __name__ == "__main__":
    main(10)
