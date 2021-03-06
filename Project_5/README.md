# Project 5: Data Pipelines with Apache Airflow on AWS

## Introduction
This project is part of the Udacity Data Engineer Nano Degree Program. The music streaming startup "Sparkify" has further grown and now wants a maintainable and scalable pipeline to stage, store and query their user and song data on Amazon Web Services (AWS). Their data are stored as JSON files in S3 buckets on AWS. Their particular interest lies on what songs their customers are listening to, which is why they need an easy to query database.

The task is to implement an Apache Airflow DAG that consist of multiple operators to stage the data from S3 buckets to Redshift, load the staged data into fact and dimension tables and perform a data quality check to confirm that the data are present.

## Datasets

The collected data consist of mainly two file types both given as JSON files:

1. Log data, which provide user activity logs from a music streaming app (generated by this [Event Simulator](https://github.com/Interana/eventsim))
2. Song data, which provide metadata about a song and its artist (subset of real data from the [Million Song Dataset](http://millionsongdataset.com/))

Log data are stored under: ``S3://udacity-dend/log_data/*/*/*.json``
<br>Song data are stored under: ``S3://udacity-dend/song_data/*/*/*/*.json``
<br>Log mapping file is stored under: ``S3://udacity-dend/log_json_path.json``

### Schema for Song Play Analysis

Airflow calls multiple operators that stages the data from S3 buckets into two staging tables. From there on, data are loaded into one fact table and four dimension tables.

#### Staging Tables

1. ``staging_events``: holds all log data
2. ``staging_songs``: holds all song data

#### Fact Table

1. ``songplays``: holds log data associated with song plays filtered using ``'NextSong'`` for page

#### Dimension Tables

1. ``users``: holds user data in app
2. ``songs``: holds songs in music library
3. ``artists``: holds artists in music library
4. ``time``: holds timestamps of song plays separated into specific units

## Program Structure

The Apache Airflow DAG is shown below: <br>
<br>
![DAG_Project5](https://github.com/mhauck-FFM/Udacity_Data_Engineering_Projects/blob/master/Project_5/DAG_Airflow_project_5.png)

<br>
The project consists of the following files (under the given branches):

```
.../airflow/dags:
  - sparkify_pipeline.py

.../airflow/plugins/operators:
  - __init__.py
  - create_tables.py
  - create_tables.sql
  - stage_redshift.py
  - load_fact.py
  - load_dimension.py
  - data_quality.py

.../airflow/plugins/helpers:
  - __init__.py
  - sql_queries.py
```

The Airflow DAG in ``sparkify_pipeline.py`` runs the following operators to execute the ETL pipeline:
  1. Begin the execution with a ``DummyOperator``
  2. Create the tables on Redshift with the ``CreateTablesOperator`` in ``create_tables.py``, which requires ``create_tables.sql``
  3. Stage data from S3 to Redshift using the ``StageToRedshiftOperator`` in ``stage_redhsift.py``
  4. Load the ``songplays`` fact table using the ``LoadFactOperator`` in ``load_fact.py``, which requires ``sql_queries.py``
  5. Load the ``songs``, ``artists``, ``users``, and ``time`` dimension tables using the ``LoadDimensionOperator`` in ``load_dimension.py``, which requires ``sql_queries.py``
  6. Perform quality checks for all fact and dimension tables using the ``DataQualityOperator`` in ``data_quality.py``
  7. Stop the execution with a ``DummyOperator``

Mandatory python modules to run the scripts:

- airflow
- helpers
- os
- datetime

Note: The solution to the project has been developed locally using only the python files (``.py``) in Python 3.6.10.

#### Instructions to run the program

To run the program, make sure to do the following preparations first:

1. Setup a Redshift cluster (you may use the scripts from [Project 2](https://github.com/mhauck-FFM/Udacity_Data_Engineering_Projects/tree/master/Project_2))
2. Setup your Airflow correctly (add your aws_credentials and redshift as connection)
3. Make sure the files are in the correct folders of your (local) Airflow distribution

Now, connect to the Airflow WebUI and the DAG should appear in the overview:

4. Activate the DAG in the WebUI and trigger it if necessary. The project should run smoothly and all operators should finish successfully

Note: Terminate the Redshift cluster after the project is done to save costs!
