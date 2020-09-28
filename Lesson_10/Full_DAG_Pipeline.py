import datetime

from airflow import DAG

from airflow.operators.udacity_plugin import (
    FactsCalculatorOperator,
    HasRowsOperator,
    S3ToRedshiftOperator
)

#
# TODO: Create a DAG which performs the following functions:
#
#       1. Loads Trip data from S3 to RedShift
#       2. Performs a data quality check on the Trips table in RedShift
#       3. Uses the FactsCalculatorOperator to create a Facts table in Redshift
#           a. **NOTE**: to complete this step you must complete the FactsCalcuatorOperator
#              skeleton defined in plugins/operators/facts_calculator.py
#
dag = DAG("lesson3.exercise4", start_date=datetime.datetime.utcnow())

copy_trips_task = S3ToRedshiftOperator(
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    table = 'trips',
    s3_bucket = 'udacity-dend',
    s3_key = 'data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv',
    task_id = 'copy_trips_data_from_s3_to_redshift',
    dag = dag
)

check_trips_task = HasRowsOperator(
    redshift_conn_id = 'redshift',
    table = 'trips',
    task_id = 'check_trips_table_data',
    dag = dag
)

calculate_facts_task = FactsCalculatorOperator(
    redshift_conn_id = 'redshift',
    origin_table = 'trips',
    destination_table = 'trips_facts',
    fact_column = 'tripduration',
    groupby_column = 'bikeid',
    task_id = 'calculate_facts_trips',
    dag = dag
)

copy_trips_task >> check_trips_task
check_trips_task >> calculate_facts_task
