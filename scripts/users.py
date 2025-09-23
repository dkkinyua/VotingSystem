import os
import uuid
import psycopg2
import psycopg2.extras as pe
import random
from dotenv import load_dotenv
from faker import Faker

load_dotenv()
pe.register_uuid()

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

        for _ in range(n):
            user_id = uuid.uuid4()
            first = fake.first_name()
            last = fake.last_name()
            email = f'{first.lower}.{last.lower}{random.randint(0, 100)}@gmail.com'
            area_id = random.randint(1, 10)
            
            cur.execute("""
                INSERT INTO votingsystem.users 
                (user_id, first_name, last_name, email, area_id, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """, (user_id, first, last, email, area_id))
            
        print("Data loaded into votingsystem.users successfully!")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error connecting psycopg2: {e}")

if __name__ == "__main__":
    main(2000)

