# Project 4 - Automate Data Pipelines with Airflow

As in the previous project this project 4 deals with the music-streaming-dataset of the startup *Sparkify*. In order schedule and monitor data pipelines **Apache Airflow** is used to orchestrate an ETL extracting data from **S3** and loading it into a **Redshift** database.

---

## Data Pipeline with Airflow

Schematic overview of the Airflow Orchestration:

BILD

1. In the *stage_events_to_redshift-DAG* and the *stage_songs_to_redshift-DAG* data is extracted from S3 and saved into the two staging tables *staging_events* and *staging_songs* in Redshift. The tables contain raw user behaviour from Sparkify and a open source song library.
2. In the *load_songplays_table-DAG* both staging tables are merged into the fact table *songplays* through an SQL query.
3. In the 4 *load_dimension-DAGs* unique values are extracted from the staging tables and are stored into the 4 dimension tables *users*, *songs*, *artists* and *time* through SQL queries.
4. In the *run_quality_checks-DAG* quality checks are performed on all 5 tables in order to ensure integrity of the data and check if the ETL ran correctly.

## Redshift Data Model

The final Redshift star schema consists of 1 fact table and 4 dimension tables.

![Redshift Data Model](./images/airflow_data_model.PNG)
