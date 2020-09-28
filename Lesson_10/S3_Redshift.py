import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import sql_statements

def load_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook('aws_credentials')
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook('redshift')
    sql_stmt = sql_statements.COPY_STATIONS_SQL.format(credentials.access_key, credentials.secret_key)
    redshift_hook.run(sql_stmt)

dag = DAG(
    'lesson1.demo6',
    start_date = datetime.datetime.now()
)

create_table = PostgresOperator(
    task_id = 'create_table',
    postgres_conn_id='redshift',
    sql = sql_statements.CREATE_STATIONS_TABLE_SQL,
    dag = dag
)

copy_task = PythonOperator(
    task_id = 'copy_to_redshift',
    python_callable = load_data_to_redshift,
    dag = dag
)

location_traffic_task = PostgresOperator(
    task_id="calculate_location_traffic",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.LOCATION_TRAFFIC_SQL
)

create_table >> copy_task
copy_task >> location_traffic_task
