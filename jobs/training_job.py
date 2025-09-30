import os
import pickle
import pandas as pd
from dotenv import load_dotenv
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from sklearn.ensemble import IsolationForest

load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PWD = os.getenv('DB_PWD')
JDBC_URL = os.getenv("JDBC_URL")

"""
steps:
1. define env_settings and table_env from imports
2. define source(votes), users, and sink table(detected_anomalies)
3. execute_sql for the three tables
4. create aliases for users.area_id and users.users_id to avoid running into errors during joining
5. join both users and area tables on area_id
6. take the resulting table and transform it to a pandas dataframe
7. transform vote_time from timestamp to bigint
8. take vote_area_id, user_area_id and vote_time as feature columns
9. declare contamination=0.05, n_estimators for no of trees and iso_forest
10. save model to model folder
11. set anomaly_score for anomaly score and anomaly as new columns
12. use from_pandas to come back to table api
13. insert data into sink table
"""
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(environment_settings=env_settings)

source_tbl = f"""
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

users_tbl = f"""
                CREATE TABLE users (
                    user_id STRING,
                    first_name STRING,
                    last_name STRING,
                    email STRING,
                    area_id INT,
                    created_at TIMESTAMP(3)
                ) WITH (
                    'connector' = 'jdbc',
                    'url' = '{JDBC_URL}',
                    'table-name' = 'votingsystem.users',
                    'username' = '{DB_USER}',
                    'password' = '{DB_PWD}',
                    'driver' = 'org.postgresql.Driver'                  
                )
             """

sink_tbl = f"""
                CREATE TABLE detected_anomalies (
                    vote_id STRING,
                    user_id STRING,
                    vote_area_id INT,
                    user_area_id INT,
                    vote_time BIGINT,
                    anomaly BOOLEAN,
                    anomaly_score DOUBLE
                ) WITH (
                    'connector' = 'jdbc',
                    'url' = '{JDBC_URL}',
                    'table-name' = 'votingsystem.detected_anomalies',
                    'username' = '{DB_USER}',
                    'password' = '{DB_PWD}',
                    'driver' = 'org.postgresql.Driver'                     
                )
            """

def train_model(source, users_tbl, sink):
    try:
        table_env.execute_sql(source)
        table_env.execute_sql(users_tbl)
        table_env.execute_sql(sink)

        votes = table_env.from_path("votes")
        users = table_env.from_path("users")
        anomalies = table_env.from_path("detected_anomalies")

        users_renamed = users.select(
            users.user_id.alias("users_user_id"),
            users.first_name,
            users.last_name,
            users.email,
            users.area_id.alias("users_area_id"),
            users.created_at
        )

        results = votes \
            .join(users_renamed, votes.user_id == users_renamed.users_user_id) \
            .select(
                votes.vote_id,
                votes.user_id,
                votes.candidate_id,
                votes.area_id.alias("vote_area_id"),
                votes.vote_time,
                votes.is_valid,
                users_renamed.users_area_id.alias("user_area_id")
            )
        
        df = results.to_pandas()
        df["vote_time"] = df["vote_time"].astype(int) // 10**9 

        features = df[["vote_area_id", "user_area_id", "vote_time"]].copy()
        features.columns = ["vote_area_id", "user_area_id", "vote_time"]
        iso_forest = IsolationForest(
            n_estimators=100,
            contamination=0.05,
            random_state=42
        )
        iso_forest.fit(features)

        with open("model/isolation_forest_model.pkl", "wb") as f:
            pickle.dump(iso_forest, f)

        print("Isolation Forest model trained and saved successfully!")
        df["anomaly_score"] = iso_forest.decision_function(features)
        df["anomaly"] = iso_forest.predict(features) == -1

        anomalies_df = df[df["anomaly"]][["vote_id", "user_id", "vote_area_id", "user_area_id", "vote_time", "anomaly", "anomaly_score"]]
        anomalies_tbl = table_env.from_pandas(anomalies_df)
        anomalies_tbl.execute_insert("detected_anomalies").wait()
        print("training flink job ran successfully!")
    except Exception as e:
        print(f"training flink job failed: {e}")

if __name__ == '__main__':
    train_model(source_tbl, users_tbl, sink_tbl)
