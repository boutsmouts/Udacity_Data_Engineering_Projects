from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import CreateTablesOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator
from airflow import DAG

from helpers.sql_queries import SqlQueries

import datetime
import os

'''
Set default arguments
'''

default_arguments = {
    'owner': 'marius',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime.datetime(2020, 9, 30, 0, 0, 0, 0),
    'end_date': datetime.datetime(2020, 10, 1, 0, 0, 0, 0),
    'schedule_interval': '@hourly',
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds = 5),
    'email_on_retry': False
}

'''
Specify DAG
'''

dag = DAG('sparkify_pipeline',
    default_args = default_arguments,
    description = 'Use Airflow to load Sparkify music streaming data from S3 to Redshift and perform data quality checks',
    max_active_runs = 3
    )

'''
Specify dummy operator at the beginning of the DAG
'''

start_operator = DummyOperator(
    task_id = 'begin_execution',
    dag = dag
    )

'''
Specify CreateTablesOperator to create tables on Redshift
'''

create_tables_in_redshift = CreateTablesOperator(
    redshift_conn_id = 'redshift',
    task_id = 'create_tables_in_redshift',
    dag = dag
)

'''
Specify StageToRedshiftOperator to stage events and song data to Redshift
'''

bucket = 'udacity-dend'
region = 'us-west-2'

stage_events_data_to_redshift = StageToRedshiftOperator(
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_events',
    s3_bucket = bucket,
    s3_key = 'log_data',
    region = region,
    file_format = 'JSON',
    log_json_key = 'log_json_path.json',
    task_id = 'stage_events_data_to_redshift',
    dag = dag
)

stage_songs_data_to_redshift = StageToRedshiftOperator(
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_songs',
    s3_bucket = bucket,
    s3_key = 'song_data',
    region = region,
    file_format = 'JSON',
    task_id = 'stage_songs_data_to_redshift',
    dag = dag
)

'''
Specify LoadFactOperator to load songplay data into fact table
'''

load_songplays_fact_table = LoadFactOperator(
    redshift_conn_id = 'redshift',
    table = 'songplays',
    sql_query = SqlQueries.songplay_table_insert,
    overwrite = True,
    task_id = 'load_songplays_fact_table',
    dag = dag
)

'''
Specify LoadDimensionOperator to load song, user, artist, and time data into dimension tables
'''

load_songs_dimension_table = LoadDimensionOperator(
    redshift_conn_id = 'redshift',
    table = 'songs',
    sql_query = SqlQueries.song_table_insert,
    overwrite = True,
    task_id = 'load_songs_dimension_table',
    dag = dag
)

load_users_dimension_table = LoadDimensionOperator(
    redshift_conn_id = 'redshift',
    table = 'users',
    sql_query = SqlQueries.user_table_insert,
    overwrite = True,
    task_id = 'load_users_dimension_table',
    dag = dag
)

load_artists_dimension_table = LoadDimensionOperator(
    redshift_conn_id = 'redshift',
    table = 'artists',
    sql_query = SqlQueries.artist_table_insert,
    overwrite = True,
    task_id = 'load_artists_dimension_table',
    dag = dag
)

load_time_dimension_table = LoadDimensionOperator(
    redshift_conn_id = 'redshift',
    table = 'time',
    sql_query = SqlQueries.time_table_insert,
    overwrite = True,
    task_id = 'load_time_dimension_table',
    dag = dag
)

perform_quality_checks = DataQualityOperator(
    redshift_conn_id = 'redshift',
    tables = ['songplays', 'songs', 'users', 'artists', 'time'],
    task_id = 'perform_quality_checks',
    dag = dag
)

'''
Specify dummy operator at the end of the DAG
'''

end_operator = DummyOperator(
    task_id = 'stop_execution',
    dag = dag
    )

'''
Order the different tasks of the DAG
'''

start_operator >> create_tables_in_redshift

create_tables_in_redshift >> [stage_events_data_to_redshift, stage_songs_data_to_redshift]
[stage_events_data_to_redshift, stage_songs_data_to_redshift] >> load_songplays_fact_table


load_songplays_fact_table >> [load_songs_dimension_table, load_users_dimension_table, load_artists_dimension_table, load_time_dimension_table]
[load_songs_dimension_table, load_users_dimension_table, load_artists_dimension_table, load_time_dimension_table] >> perform_quality_checks

perform_quality_checks >> end_operator
