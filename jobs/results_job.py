import os
from dotenv import load_dotenv
from pyflink.table import TableEnvironment, EnvironmentSettings

load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PWD = os.getenv('DB_PWD')
JDBC_URL = os.getenv("JDBC_URL")
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

'''
steps:
1. define the source table, votes
2. define candidates source table for joining with votes
3. define the sink table, results
4. initialize table_env
5. use execute_sql() method to define and create these tables in Flink
6. get total votes per candidate, grouping by candidate_id as there's only one candidate per area, 
   so we can't group per area
7. join the candidates table to votes table through candidate_id
8. get rank of candidates based on total votes
9. push results to the results table using execute_insert()
'''

source_table = f"""
                CREATE TABLE votes (
                    vote_id STRING,
                    user_id STRING,
                    candidate_id INT,
                    area_id INT,
                    vote_time TIMESTAMP(3),
                    is_valid BOOLEAN
                ) WITH (
                    'connector' = 'jdbc',
                    'url' = '{JDBC_URL}',
                    'table-name' = 'votingsystem.votes',
                    'username' = '{DB_USER}',
                    'password' = '{DB_PWD}',
                    'driver' = 'org.postgresql.Driver'
                )
                """

candidates_source = f"""
                        CREATE TABLE candidates (
                            candidate_id INT,
                            name STRING,
                            area_id INT
                        ) WITH (
                            'connector' = 'jdbc',
                            'url' = '{JDBC_URL}',
                            'table-name' = 'votingsystem.candidates',
                            'username' = '{DB_USER}',
                            'password' = '{DB_PWD}',
                            'driver' = 'org.postgresql.Driver'
                        )
                     """

sink_table = f"""
                CREATE TABLE results (
                    candidate_id INT,
                    name STRING,
                    total_votes BIGINT,
                    area_id INT,
                    candidate_rank BIGINT
                ) WITH (
                    'connector' = 'jdbc',
                    'url' = '{JDBC_URL}',
                    'table-name' = 'votingsystem.results',
                    'username' = '{DB_USER}',
                    'password' = '{DB_PWD}',
                    'driver' = 'org.postgresql.Driver'                   
                )
              """

def analyze_results(source, candidate_src, sink):
    try:
        table_env.execute_sql(source)
        table_env.execute_sql(sink)
        table_env.execute_sql(candidate_src)

        query = """
                    INSERT INTO results
                    SELECT
                        c.candidate_id,
                        c.name,
                        COUNT(v.vote_id) AS total_votes,
                        c.area_id,
                        RANK() OVER (ORDER BY COUNT(v.vote_id) DESC) AS candidate_rank
                    FROM votes v
                    JOIN candidates c
                        ON v.candidate_id = c.candidate_id
                    WHERE v.is_valid = TRUE
                    GROUP BY c.candidate_id, c.name, c.area_id
                """
        
        table_env.execute_sql(query).wait()
        print("Result flink job has run successfully!")
    except Exception as e:
        print(f"Results flink job failed: {e}")
        raise

if __name__ == '__main__':
    analyze_results(source_table, candidates_source, sink_table)