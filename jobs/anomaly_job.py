import os
from dotenv import load_dotenv
from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col, lit

load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PWD = os.getenv('DB_PWD')
JDBC_URL = os.getenv("JDBC_URL")

'''
Steps:
1. Declare env_settings from pyflink.table.EnvironmentSettings in batch mode as i'm using table API for batch processing
2. Set table_env deriving from TableEnvironment and env_settings
3. Since our data is in a psql table, i'll use DDL statements to create a table and define our source table, votes.
4. Define our sink table, analyzed_anomalies
5. Execute DDL statements using execute_sql() function.
6. Set votes using from_path
7. Get the result by filtering data where the is_valid flag = False
8. Insert into sink table using execute_insert(table_name)
9. If the job runs successfully, print success message, else raise error
'''

env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(environment_settings=env_settings)

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

sink_table = f"""
                CREATE TABLE analyzed_anomalies (
                    vote_id STRING,
                    user_id STRING,
                    candidate_id INT,
                    area_id INT,
                    vote_time TIMESTAMP(3),
                    is_valid BOOLEAN,
                    reason STRING
                ) WITH (
                    'connector' = 'jdbc',
                    'url' = '{JDBC_URL}',
                    'table-name' = 'votingsystem.analyzed_anomalies',
                    'username' = '{DB_USER}',
                    'password' = '{DB_PWD}',
                    'driver' = 'org.postgresql.Driver'
                )
            """

def analyze(source_table, sink_table):
    try:
        table_env.execute_sql(source_table)
        table_env.execute_sql(sink_table)

        votes = table_env.from_path("votes")
        result = votes.where(col("is_valid") == False) \
                    .add_or_replace_columns(lit("Flagged as an invalid vote").alias("reason"))
        result.execute_insert("analyzed_anomalies").wait()
        print('Anomaly Flink job complete')
    except Exception as e:
        print(f"Anomaly Flink job failed: {e}")
        raise

if __name__ == '__main__':
    analyze(source_table, sink_table)


