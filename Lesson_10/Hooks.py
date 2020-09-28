import datetime
import logging
import botocore

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

def list_keys():
    hook = S3Hook(aws_conn_id = 'aws_credentials')
    bucket = Variable.get('dend_udacity')
    logging.info(f'Listing keys from {bucket}')
    keys = hook.list_keys(bucket)
    for key in keys:
        logging.info(f'- s3://{bucket}/{key}')

def hello_date(*args, **kwargs):
    print(f'Hello {kwargs["execution_date"]}')

dag = DAG(
    'lesson1.demo4',
    start_date = datetime.datetime.now() - datetime.timedelta(days = 365),
    end_date = datetime.datetime(2020, 10, 1, 0, 0, 0, 0),
    schedule_interval = '@monthly',
    max_active_runs = 1
)

list_task = PythonOperator(
    task_id = 'list_task',
    python_callable = list_keys,
    dag = dag
)

dag_2 = DAG(
    'lesson1.demo5',
    start_date = datetime.datetime.now() - datetime.timedelta(days = 365),
    end_date = datetime.datetime(2020, 10, 1, 0, 0, 0, 0),
    schedule_interval = '@monthly',
    max_active_runs = 1
)

task = PythonOperator(
    task_id = 'hello_date',
    python_callable = hello_date,
    provide_context = True,
    dag = dag_2
)
