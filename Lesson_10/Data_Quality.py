import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import sql_statements

def load_trip_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook('aws_credentials')
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook('redshift')
    execution_date = kwargs['execution_date']
    sql_stmt = sql_statements.COPY_MONTHLY_TRIPS_SQL.format(
        credentials.access_key,
        credentials.secret_key,
        year = execution_date.year,
        month = execution_date.month
    )
    redshift_hook.run(sql_stmt)

def load_station_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook('aws_credentials')
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook('redshift')
    execution_date = kwargs['execution_date']
    sql_stmt = sql_statements.COPY_STATIONS_SQL.format(
        credentials.access_key,
        credentials.secret_key
    )
    redshift_hook.run(sql_stmt)

def check_greater_than_zero(*args, **kwargs):
    table = kwargs['params']['table']
    redshift_hook = PostgresHook('redshift')
    records = redshift_hook.get_records(f'SELECT COUNT(*) FROM {table}')

    if records is None or len(records[0]) < 1:
        logging.error(f'No records presents in destination table {table}')
        raise ValueError(f'No records presents in destination table {table}')

    logging.info(f'Data quality on table {table} check passed with {records[0][0]} records')

dag = DAG(
    'lesson1.demo8',
    start_date = datetime.datetime(2018,1,1,0,0,0,0),
    end_date = datetime.datetime(2018,1,1,0,0,0,0),
    schedule_interval = '@monthly',
    max_active_runs = 1
)

create_trips_table = PostgresOperator(
    task_id = 'create_trips_table',
    postgres_conn_id='redshift',
    sql = sql_statements.CREATE_TRIPS_TABLE_SQL,
    dag = dag
)

copy_trips_task = PythonOperator(
    task_id = 'copy_trips_to_redshift',
    python_callable = load_trip_data_to_redshift,
    dag = dag,
    provide_context = True,
    sla = datetime.timedelta(hours = 1)
)

check_trips = PythonOperator(
    task_id = 'check_trips_data',
    dag = dag,
    python_callable = check_greater_than_zero,
    provide_context = True,
    params = {
        'table': 'trips'
    }
)

create_stations_table = PostgresOperator(
    task_id = 'create_stations_table',
    postgres_conn_id='redshift',
    sql = sql_statements.CREATE_STATIONS_TABLE_SQL,
    dag = dag
)

copy_stations_task = PythonOperator(
    task_id = 'copy_stations_to_redshift',
    python_callable = load_station_data_to_redshift,
    dag = dag
)

check_stations = PythonOperator(
    task_id = 'check_stations_data',
    dag = dag,
    python_callable = check_greater_than_zero,
    provide_context = True,
    params = {
        'table': 'stations'
    }
)

create_trips_table >> copy_trips_task
create_stations_table >> copy_stations_task

copy_trips_task >> check_trips
copy_stations_task >> check_stations
