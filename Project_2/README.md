# Project 3: Data Warehouse on AWS

## Introduction
This project is part of the Udacity Data Engineer Nano Degree Program. The music streaming startup "Sparkify" has significantly grown and now wants a cloud data warehouse to store and query their user and song data on Amazon Redshift. Their data are stored as JSON files in S3 buckets on AWS. Their particular interest lies on what songs their customers are listening to, which is why they need an easy to query database.

The task is to setup a Redshift cluster, create the data warehouse and fill it with the desired data to perform analytical queries.

## Datasets

The collected data consist of mainly two file types both given as JSON files:

1. Log data, which provide user activity logs from a music streaming app (generated by this [Event Simulator](https://github.com/Interana/eventsim))
2. Song data, which provide metadata about a song and its artist (subset of real data from the [Million Song Dataset](http://millionsongdataset.com/))

### Schema for Song Play Analysis

To ingest the data into the data warehouse, data of the raw JSON files are copied from S3 into two staging tables on the Redshift cluster. From there on, the data is transformed into a star schema with one fact table and four dimension tables in total.

#### Staging Tables

1. staging_events: holds all log data from the JSON log files
    - artist (VARCHAR)
    - auth (VARCHAR)
    - firstName (VARCHAR)
    - gender (VARCHAR)
    - ItemInSession (INT)
    - lastName (VARCHAR)
    - length (FLOAT)
    - level (VARCHAR)
    - location (VARCHAR)
    - method (VARCHAR)
    - page (VARCHAR)
    - registration (DOUBLE PRECISION)
    - sessionId (INT)
    - song (VARCHAR)
    - status (INT)
    - ts (TIMESTAMP)
    - userAgent (VARCHAR)
    - userId (INT)

2. staging_songs: holds all song data from the JSON song files
    - artist_id (VARCHAR)
    - artist_latitude (FLOAT)
    - artist_longitude (FLOAT)
    - artist_location (VARCHAR)
    - artist_name (VARCHAR)
    - song_id (VARCHAR)
    - title (VARCHAR)
    - duration (FLOAT)
    - year (INT)


#### Fact Table

1. songplays: holds log data associated with song plays filtered using ``'NextSong'`` for page
    - songplay_id (INT - IDENTITY (0, 1))
    - start_time (TIMESTAMP)
    - user_id (INT)
    - level (VARCHAR)
    - song_id (VARCHAR)
    - artist_id (VARCHAR)
    - session_id (INT)
    - location (VARCHAR)
    - user_agent (VARCHAR)

songplay_id is the PRIMARY KEY for the songplays table. As it might be useful to JOIN the data based on the song_id, song_id is used as DISTKEY and SORTKEY.

#### Dimension Tables

1. users: holds user data in app
    - user_id (INT)
    - firt_name (VARCHAR)
    - last_name (VARCHAR)
    - gender (VARCHAR)
    - level (VARCHAR)

user_id is the PRIMARY KEY for the users table. As it might be useful to perform JOIN on the user_id, DISTSTYLE is set to all.

2. songs: holds songs in music library
    - song_id (INT)
    - title (VARCHAR)
    - artist_id (VARCHAR)
    - year (INT)
    - duration (FLOAT)

song_id is the PRIMARY KEY for the songs table. As above, JOINS might be done excessively on the song_id, so that song_id is used as DISTKEY, while SORTKEY is specified to be the year.

3. artists: holds artists in music library
    - artist_id (VARCHAR)
    - name (VARCHAR)
    - location (VARCHAR)
    - latitude (FLOAT)
    - longitude (FLOAT)

artist_id is the PRIMARY KEY for the artists table. As it might be useful to perform JOIN the artist_id, DISTSTYLE is set to all.

4. time: holds timestamps of song plays separated into specific units
    - start_time (TIMESTAMP)
    - hour (INT)
    - day (INT)
    - week (INT)
    - month (INT)
    - year (INT)
    - weekday (INT)

start_time is the PRIMARY KEY for the time table. As it might be useful to perform JOIN on the start_time, DISTSTYLE is set to all.

## Program Structure

The following files are included for this project:

  1. ``sql_queries.py``, used to store necessary SQL queries to create, copy, fill and query the tables
  2. ``create_tables.py``, used to drop and recreate desired tables; requires ``sql_queries.py``
  3. ``etl.py``, used to read, process and store the given JSON song and log files in the database; requires ``sql_queries.py``
  4. ``setup_redshift.py``, used to setup a Amazon Redshift cluster including IAM user role and VPC group; requires ``dwh.cfg``
  5. ``delete_redshift.py``, used to delete the created Amazon Redshift cluster including IAM user role and VPC group; requires ``dwh.cfg``, ``sql_queries.py``, and ``setup_redshift.py``
  6. ``dwh.cfg``, used to parse AWS settings and credentials
  7. ``sample_queries.py``, used to run two sample queries (OPTIONAL!)

Mandatory python modules to run the scripts:

- boto3
- botocore.exceptions
- json
- configparser
- time
- psycopg2
- pandas

Note: The solution to the project has been developed locally using only the python files (``.py``) in Python 3.6.10.

#### Instructions to run the program

Use the following steps to create the desired data warehouse for the project. Repeat if necessary.

1. Insert your AWS settings and user credentials into ``dwh.cfg`` at empty spaces:
    ```
    [AWS]
    key =
    secret =

    [CLUSTER]
    host = UPDATED_AUTOMATICALLY
    db_name = db
    db_user = db_usr
    db_password = P4ssw0rd
    db_port = 5439
    identifier =
    type = multi-node
    num_nodes = 4
    node_type = dc2.large
    region = us-west-2
    security_group_name =

    [IAM]
    policy_arn = arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
    name =
    role_arn = UPDATED_AUTOMATICALLY

    [S3]
    log_data = 's3://udacity-dend/log_data'
    log_jsonpath = 's3://udacity-dend/log_json_path.json'
    song_data = 's3://udacity-dend/song_data'
    ```
2. Run ``setup_redshift.py`` in your terminal using: ``python setup_redshift.py``
3. Run ``create_tables.py`` in your terminal using: ``python create_tables.py``
4. Run ``etl.py`` in your terminal using: ``python etl.py``
5. Perform some awesome analytics with the data! (e.g., use ``sample_queries.py``)
6. Run ``delete_redshift.py`` in your terminal using: ``python delete_redshift.py``

Note: For successfully running these four scripts, you may need to adjust the ``pwd`` of your terminal to the folder where ``create_tables.py``, ``sql_queries.py``, ``etl.py``, ``setup_redshift.py``, ``delete_redshift.py``, ``dwh.cfg``, and ``sample_queries.py`` are stored.