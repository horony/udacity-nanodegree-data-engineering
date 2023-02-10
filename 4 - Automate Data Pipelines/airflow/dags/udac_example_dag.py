from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
import os
from airflow.operators import (LoadDimensionOperator, StageToRedshiftOperator, LoadFactOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'lennart',
    'start_date': datetime(2019, 1, 1),
    'end_date': datetime(2019, 1, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'max_active_runs': 1
    }

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Sparkify ETL: S3 to Redshift',
          schedule_interval='0 * * * *' # Schedule: Every hour
         ) 

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag )

# loads event data from S3 to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table_name="staging_events",
    s3_bucket = "udacity-dend",
    s3_key = "log_data",
    file_format="JSON",
    log_json_file = "log_json_path.json",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    provide_context=True
)

# loads song data from S3 to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table_name="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/",
    file_format="JSON",   
    redshift_conn_id = "redshift",
    aws_credential_id = "aws_credentials",   
    provide_context=True
)

# creates fact table songplays from stage data within Redshift
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table_name = "songplays",
    redshift_conn_id = "redshift",
    aws_credential_id = "aws_credentials",   
    provide_context=True,
    sql_query = SqlQueries.songplay_table_insert
)

# creates dimension table users from stage data within Redshift
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table_name = "users",
    redshift_conn_id = "redshift",
    provide_context=True,
    sql_query = SqlQueries.user_table_insert,
    mode = 'append'
)

# creates dimension table songs from stage data within Redshift
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table_name = "songs",
    redshift_conn_id = "redshift",
    provide_context=True,
    sql_query = SqlQueries.song_table_insert,
    mode = 'append'
)

# creates dimension table artists from stage data within Redshift
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table_name = "artists",
    redshift_conn_id = "redshift",
    provide_context=True,
    sql_query = SqlQueries.artist_table_insert,
    mode = 'append'

)

# creates dimension table time from stage data within Redshift
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table_name = "time",
    redshift_conn_id = "redshift",
    provide_context=True,
    sql_query = SqlQueries.time_table_insert,
    mode = 'append'
)

# carries out quality checks on previously created tables
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dict_tests = [{'table': 'songplays', 'min_rows': 1},
                  {'table': 'users', 'min_rows': 1},
                  {'table': 'songs', 'min_rows': 1},
                  {'table': 'artists', 'min_rows': 1},
                  {'table': 'time', 'min_rows': 1}]
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag )

# DAG dependencies
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator