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
    'start_date': datetime.datetime(2020, 10, 1, 0, 0, 0, 0),
    'end_date': datetime.datetime(2020, 10, 2, 0, 0, 0, 0),
    'schedule_interval': '@hourly',
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds = 5),
    'email_on_retry': False
}

'''
Specify DAG
'''

dag = DAG('capstone_pipeline',
    default_args = default_arguments,
    description = 'Use Airflow to load New York car crash and weather data from S3 to Redshift, perform data quality checks and SQL queries',
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

bucket = 'mh-udacity-dend'
region = 'eu-central-1'

stage_crash_data_to_redshift = StageToRedshiftOperator(
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_crashes',
    s3_bucket = bucket,
    s3_key = 'capstone_project/crash_data',
    region = region,
    file_format = 'JSON',
    log_json_key = 'capstone_project/crash_json_path.json',
    task_id = 'stage_crash_data_to_redshift',
    dag = dag
)

stage_temperature_data_to_redshift = StageToRedshiftOperator(
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_temperature',
    column_list = '(datetime, "New York")',
    s3_bucket = bucket,
    s3_key = 'capstone_project/weather_data/temperature.csv',
    region = region,
    file_format = 'CSV',
    task_id = 'stage_temperature_data_to_redshift',
    dag = dag
)

stage_humidity_data_to_redshift = StageToRedshiftOperator(
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_humidity',
    column_list = '("datetime", "New York")',
    s3_bucket = bucket,
    s3_key = 'capstone_project/weather_data/humidity.csv',
    region = region,
    file_format = 'CSV',
    task_id = 'stage_humidity_data_to_redshift',
    dag = dag
)

stage_pressure_data_to_redshift = StageToRedshiftOperator(
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_pressure',
    column_list = '("datetime", "New York")',
    s3_bucket = bucket,
    s3_key = 'capstone_project/weather_data/pressure.csv',
    region = region,
    file_format = 'CSV',
    task_id = 'stage_pressure_data_to_redshift',
    dag = dag
)

'''
Perform data quality check for staging tables
'''

perform_staging_quality_checks = DataQualityOperator(
    redshift_conn_id = 'redshift',
    tables = ['staging_crashes', 'staging_temperature', 'staging_humidity', 'staging_pressure'],
    task_id = 'perform_staging_quality_checks',
    dag = dag
)

'''
Specify LoadFactOperator to load songplay data into fact table
'''

load_incidents_fact_table = LoadFactOperator(
    redshift_conn_id = 'redshift',
    table = 'incidents',
    sql_query = SqlQueries.incidents_table_insert,
    overwrite = True,
    task_id = 'load_incidents_fact_table',
    dag = dag
)

'''
Specify LoadDimensionOperator to load song, user, artist, and time data into dimension tables
'''

load_locations_dimension_table = LoadDimensionOperator(
    redshift_conn_id = 'redshift',
    table = 'locations',
    sql_query = SqlQueries.locations_table_insert,
    overwrite = True,
    task_id = 'load_locations_dimension_table',
    dag = dag
)

load_weather_dimension_table = LoadDimensionOperator(
    redshift_conn_id = 'redshift',
    table = 'weather',
    sql_query = SqlQueries.weather_table_insert,
    overwrite = True,
    task_id = 'load_weather_dimension_table',
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

'''
Perform data quality check for fact and dimension tables
'''

perform_fact_dimension_quality_checks = DataQualityOperator(
    redshift_conn_id = 'redshift',
    tables = ['incidents', 'locations', 'weather', 'time'],
    task_id = 'perform_fact_dimension_quality_checks',
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

create_tables_in_redshift >> [stage_crash_data_to_redshift, stage_temperature_data_to_redshift, stage_humidity_data_to_redshift, stage_pressure_data_to_redshift]
[stage_crash_data_to_redshift, stage_temperature_data_to_redshift, stage_humidity_data_to_redshift, stage_pressure_data_to_redshift] >> perform_staging_quality_checks

perform_staging_quality_checks >> load_incidents_fact_table

load_incidents_fact_table >> [load_locations_dimension_table, load_weather_dimension_table, load_time_dimension_table]
[load_locations_dimension_table, load_weather_dimension_table, load_time_dimension_table] >> perform_fact_dimension_quality_checks

perform_fact_dimension_quality_checks >> end_operator
