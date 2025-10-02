# Voting Anomaly Detection Pipeline

This project implements a real-time data pipeline for analyzing voting data, detecting anomalies, and persisting results into a PostgreSQL database using **Apache Flink**.  

The system is designed to handle high-velocity streaming data from a voting system, perform anomaly detection (such as abnormal voting rates), and provide insights via dashboards.  

---

## Table of Contents
1. [Project Architecture](#project-architecture)  
2. [Technologies Used](#technologies-used)  
3. [Database Schema](#database-schema)  
4. [Data Pipeline Flow](#data-pipeline-flow)  
5. [Setup Instructions](#setup-instructions)  
6. [Running the Job](#running-the-job)  
7. [Dashboard Snapshots](#dashboard-snapshots)  
8. [Future Enhancements](#future-enhancements)  

---

## Project Architecture

The pipeline follows a **streaming architecture**:  

1. **Data Ingestion**: Voting data (`votes` table) is streamed into Apache Flink (via CDC connector or direct JDBC read).  
2. **Transformation & Anomaly Detection**:  
   - Detects duplicate votes (same `user_id` for a candidate).  
   - Identifies abnormal voting rates within a time window.  
   - Performs aggregations by `candidate_id` and `area_id`.  
3. **Sink**: Results are written to PostgreSQL tables:  
   - `results` (aggregated vote counts per candidate/area)  
   - `analyzed_anomalies` (suspicious voting activity)  
4. **Visualization**: Dashboards (Grafana/Power BI/Metabase) display real-time insights.  

### Project and Schema Architecture Diagrams

To create the PostgreSQL tables on any admin applications e.g. dbeaver or pgAdmin4, use the SQL query code in `sql/schema.sql` to do so.

**INSERT ARCHITECTURE AND SCHEMA DIAGRAMS HERE**

## Technologies Used

- **Apache Flink** (1.17.x) – Streaming engine  
- **PostgreSQL** – Storage for votes, results, and anomalies  
- **Flink CDC Connector** – (Optional) For real-time DB changes  
- **Grafana / Power BI / Metabase** – Dashboard visualization  
- **Python API for Flink, `pyflink`** – Job definition  

## Project Setup and Instructions
This project runs on Flink `1.17.2`.
### 1. Clone this repository
```bash
https://github.com/dkkinyua/VotingSystem.git
cd VotingSystem
```

### 2. Setup virtual envionment and install dependencies
```bash
python3 -m venv myenv
pip install -r requirements.txt
```

#### a. Install Flink in your machine
**1.Install at your home folder, not project root directory using this command**
```bash
wget https://archive.apache.org/dist/flink/flink-1.17.2/flink-1.17.2-bin-scala_2.12.tgz
```
**2. Extract the files**
```bash
tar -xzf flink-1.17.2-bin-scala_2.12.tgz
```
**3. Rename the Flink folder to `flink/` for easier access**
```bash
mv flink-1.17.2-bin-scala_2.12.tgz flink
```
**4. Set environment variables**
```bash
export FLINK_HOME=~/flink
export PATH=$FLINK_HOME/bin:$PATH
```

#### b. Install Flink JDBC Connector and PostgreSQL JAR files
For the Flink jobs to run successfully, you need to install JAR files from the Maven repository. This is because Flink for Python has important Java dependencies that enable the jobs to run.

Download the Flink JDBC Connector JAR file `flink-connector-jdbc-3.1.2-1.17.jar ` [here](https://mvnrepository.com/artifact/org.apache.flink/flink-connector-jdbc/3.1.2-1.17) and the PostgreSQL JAR file `postgresql-42.6.0.jar` [here](https://mvnrepository.com/artifact/org.postgresql/postgresql/42.6.0).

To install these JAR files, run the following command in the root directory.

```bash
# For PostgreSQL
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# For Flink JDBC Connector
wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc_2.12/1.17.2/flink-connector-jdbc_2.12-1.17.2.jar
```
#### c. Copy these JAR files to `$FLINK_HOME/lib`
```bash
cp flink-connector-jdbc_2.12-1.17.2.jar $FLINK_HOME/lib/
cp postgresql-42.6.0.jar $FLINK_HOME/lib/
```
Now you can run these jobs normally using `python3 job_name.py` without setting the JAR files and using `flink run -C 'postgresql-42.6.0.jar' -C 'flink-connector-jdbc-3.1.2-1.17.jar' -py job_name.py` command to run these jobs.





