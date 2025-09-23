import os
import uuid
import random
import psycopg2
import psycopg2.extras as pe
from dotenv import load_dotenv
from datetime import datetime, timezone
from psycopg2.extras import execute_values

load_dotenv()
pe.register_uuid()
DB_USER = os.getenv('DB_USER')
DB_PWD = os.getenv('DB_PWD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

"""
Steps:
1. get_user_ids gets user_id and area_id (default limit 10,000).
2. get_candidate_ids fetches all candidate IDs.
3. get_area_ids fetches all area IDs.
4. generate_votes generates real and anomalous votes, real votes and, invalid can have wrong area, or wrong time (after 2am - 4am), or both.
5. insert_votes inserts into the votes table in batch.
"""

def get_user_ids(cur, limit=10000):
    cur.execute(
        """
        SELECT user_id, area_id FROM votingsystem.users
        LIMIT %s
        """,
        (limit,)
    )
    return cur.fetchall()

def get_candidate_ids(cur):
    cur.execute("SELECT candidate_id FROM votingsystem.candidates")
    return [row[0] for row in cur.fetchall()]

def get_area_ids(cur):
    cur.execute("SELECT area_id FROM votingsystem.areas")
    return [row[0] for row in cur.fetchall()]

def random_time(now_utc):
    """Return datetime today with a random time between 02:00 and 04:59 UTC."""
    hour = random.randint(2, 4)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return now_utc.replace(hour=hour, minute=minute, second=second, microsecond=0)

def generate_votes(cur, real=9500, invalid=500, invalid_time_frac=0.3):
    """
    invalid_time_frac: fraction of invalid votes that should have a time after 2AM.
    """
    users = get_user_ids(cur, limit=10000)
    candidate_ids = get_candidate_ids(cur)
    all_area_ids = get_area_ids(cur)

    votes = []
    normal_votes = random.sample(users, real)
    invalid_votes = random.sample(users, invalid)

    now = datetime.now(timezone.utc)

    # real votes
    for user_id, area_id in normal_votes:
        vote_id = uuid.uuid4()
        candidate_id = random.choice(candidate_ids)
        votes.append((vote_id, user_id, candidate_id, area_id, now, True))

    # invalid votes
    for user_id, user_area in invalid_votes:
        vote_id = uuid.uuid4()
        candidate_id = random.choice(candidate_ids)

        # pick wrong area different from registered one (if available)
        possible_areas = [a for a in all_area_ids if a != user_area]
        if possible_areas:
            area_id = random.choice(possible_areas)
        else:
            area_id = user_area  # fallback

        # some invalid votes happen after 2AM
        if random.random() < invalid_time_frac:
            vote_time = random_time(now)
        else:
            vote_time = now

        votes.append((vote_id, user_id, candidate_id, area_id, vote_time, False))

    return votes

def insert_votes(cur, conn, votes):
    try:
        query = """
            INSERT INTO votingsystem.votes
            (vote_id, user_id, candidate_id, area_id, vote_time, is_valid)
            VALUES %s
        """
        execute_values(cur, query, votes)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error inserting values into votes: {e}")

if __name__ == '__main__':
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PWD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()
        votes = generate_votes(cur, real=9500, invalid=500)
        insert_votes(cur, conn, votes)
        print("Inserted data into votes successfully!")
    except Exception as e:
        print(f"Error: {e}")
