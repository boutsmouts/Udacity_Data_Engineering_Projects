# Project 7: Capstone Project

# Introduction

New York City is one of the busiest cities in the world with circa 8.5 million citizens and almost 20 million citizens in its metropolitan area (see [Wikipedia](https://de.wikipedia.org/wiki/New_York_City)). This implies that the current traffic situation within the city is quite heavy, which inevitably leads to traffic accidents all over the city. It is likely that that the occurence of these car crashes and accidents accumulates around main traffic axes of the city and follows certain temporal patterns. Additionally, the weather conditions might also have an influence on these accidents.

The focus of this Capstone Project is to build a cloud data warehouse in Amazon Redshift using data on S3, fill it with suitable datasets on a scalable time scale using Apache Airflow and run SQL queries against the database on AWS Redshift. AWS Redshift is selected, as it is a highly flexible yet structured database solution that can handle significant amounts of data easily. Apache Airflow is introduced to create a scalable workflow that grows with the data and can be scheduled easily.

The goal of this project is to answer the following questions:
  - What are the months with the most car crashes reported?
  - What are the weekdays and hours of the day with the most crashes reported?
  - What are the streets where the most crashes occur?
  - Can we find relations between the prevalent weather condition and the car crashes?
  - What are the years with the most injuries reported?

# Datasets

To answer these questions, two datasets are used and loaded into the data warehouse. The data are:

1. Real reported car crash data in New York City (freely available under [this](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) link). Consists of almost 1.8 million rows of raw data.

2. Real reported temperature, humidity and pressure data of New York City (freely available under [this](https://www.kaggle.com/selfishgene/historical-hourly-weather-data) link). Consists of circa 60000 rows of raw data each.

The data are stored as two different file types in an S3 bucket and can be found here:

1. CRASH DATA as JSON: ``S3://mh-udacity-dend/capstone_project/crash_data/*.json``
2. WEATHER DATA as CSV: ``S3://mh-udacity-dend/capstone_project/weather_data/*.csv``

# Data Model & Data Dictionary

To gain the above defined insights, the data are first loaded from the S3 bucket into four staging tables, which are designed as follows:

1. ``staging_crashes``: holds all crash data from the files
   - collision_id (int, PRIMARY KEY): unique identifier of each crash
   - date (varchar): the date of the reported crash
   - time (varchar): the time of the reported crash
   - borough (varchar): the borough of the crash location
   - zip (varchar): the zip code of the crash location
   - street (varchar): the street name of the crash location
   - latitude (float): geographical latitude of the crash location
   - longitude (float): geographical longitude of the crash location
   - injuries (float): number of injuries of the reported crash
   - fatalities (float): number of fatalities of the reported crash
   - cause_vehicle_1 (varchar): reported cause of the crash for involved vehicle #1
   - cause_vehicle_2 (varchar): reported cause of the crash for involved vehicle #2
   - cause_vehicle_3 (varchar): reported cause of the crash for involved vehicle #3
   - cause_vehicle_4 (varchar): reported cause of the crash for involved vehicle #4
   - cause_vehicle_5 (varchar): reported cause of the crash for involved vehicle #5


2. ``staging_temperature``: holds all air temperature data from the files
   - datetime (varchar, PRIMARY KEY): unique timestamp of temperature reporting
   - New York (float): reported air temperature for New York City


3. ``staging_humidity``: holds all relative humidity data from the files
    - datetime (varchar, PRIMARY KEY): unique timestamp of humidity reporting
    - New York (float): reported relative humidity for New York City


4. ``staging_pressure``: holds all air pressure data from the files
   - datetime (varchar, PRIMARY KEY): unique timestamp of pressure reporting
   - New York (float): reported air pressure for New York City

After the staging process, the data from the staging tables are loaded into a star schema with one fact table and three related dimension tables. The tables are designed as follows and the star schema can be seen in the picture below. Note that the data are filtered so that ``latitude`` and ``longitude`` are not NULL and unequal to ``0.0``:

1. FACT: ``incidents``: holds the incident facts
   - collision_id (int, PRIMARY KEY): unique identifier of each crash
   - timestamp (timestamp): timestamp of each crash
   - location_id (timestamp): an identifier for the crash location
   - injuries (float): number of injuries in the crash
   - fatalities (float): number of fatalities in the crash
   - cars_involved (float): number of cars involved (between 1 and 5)
   - causes (varchar): causing factor of the crash for each involved car


2. DIMENSION: ``locations``:  holds information about the crash location
   - location_id (varchar, PRIMARY KEY): unique identifier of the crash location
   - borough (varchar): the borough of the crash location
   - zip (varchar): the zip code of the crash location
   - street (varchar): the street name of the crash location
   - latitude (float): geographical latitude of the crash location
   - longitude (float): geographical longitude of the crash location


3. DIMENSION: ``weather``: holds weather data for the timestamps
   - timestamp (timestamp, PRIMARY KEY): unique timestamp
   - temperature (float): air temperature reported in K (273.15 K == 0 °C)
   - humidity (float): relative humidity reported in %
   - pressure (float): air pressure reported in hPa (1013.25 hPa == standard pressure == 1 bar)


4. DIMENSION: ``time``: holds information about each timestamp
   - timestamp (timestamp, PRIMARY KEY): unique timestamp
   - hour (int): hour of the timestamp
   - day (int): day of the timestamp
   - month (varchar): month of the timestamp
   - year (int): year of the timestamp
   - weekday (varchar): weekday of the timestamp (0 == Sunday)

Here is an overview of the star schema:

![Star_Schema_Capstone](https://github.com/mhauck-FFM/Udacity_Data_Engineering_Projects/blob/master/Project_7/Star_Schema_Capstone.png)

# Program Structure

For full scalability on size and time scale of the ETL pipeline and data warehouse, the process is designed to be a DAG in Apache Airflow. The DAG is structured as follows:

![DAG_Capstone](https://github.com/mhauck-FFM/Udacity_Data_Engineering_Projects/blob/master/Project_7/capstone_DAG.png)

The project consists of the following files (under the given branches):

```
...:
  - setup_redshift.py
  - run_sql_insights.py
  - delete_redshift.py
  - dwh.cfg

.../airflow/dags:
  - capstone_pipeline.py

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
  4. Perform quality checks for all staging tables (``staging_crashes``, ``staging_temperature``, ``staging_humidity``, and ``staging_pressure``) using the ``DataQualityOperator`` in ``data_quality.py``
  5. Load the ``incidents`` fact table using the ``LoadFactOperator`` in ``load_fact.py``, which requires ``sql_queries.py``
  6. Load the ``locations``, ``weather``, and ``time`` dimension tables using the ``LoadDimensionOperator`` in ``load_dimension.py``, which requires ``sql_queries.py``
  7. Perform quality checks for all fact and dimension tables using the ``DataQualityOperator`` in ``data_quality.py``
  8. Stop the execution with a ``DummyOperator``

To ease the setup and queries for the user, the program also features four more relevant files:
  1. ``dwh.cfg``, a config file for AWS credentials and Redshift settings
  2. ``setup_redshift.py``, a python script to setup a Redshift cluster using the settings in ``dwh.cfg``
  3. ``run_sql_insights.py``, a python script to run the desired SQL queries (uses ``dwh.cfg``)
  4. ``delete_redshift.py``, a python script to delete the Redshift cluster (uses ``dwh.cfg``)

Mandatory python modules to run the scripts:

- airflow
- helpers
- os
- datetime
- pandas
- psycopg2
- configparser

Note: The solution to the project has been developed locally using only the python files (``.py``) in Python 3.6.10.

# Instructions to run the program

To run the program, make sure to do the following preparations first:

1. Setup a Redshift cluster (insert your credentials into ``dwh.cfg`` and run ``setup_redshift.py`` in your local terminal)
2. Setup your Airflow correctly (add your aws_credentials and redshift as connection)
3. Make sure the files are in the correct folders of your (local) Airflow distribution

Now, connect to the Airflow WebUI and the DAG should appear in the overview:

4. Activate the DAG in the WebUI and trigger it if necessary. The project should run smoothly and all operators should finish successfully
5. Run some awesome SQL queries and gain insights! (run ``run_sql_insights.py`` in your terminal)

Note: Terminate the Redshift cluster after the project is done to save costs (execute ``delete_redshift.py`` in your local terminal)!

# Gained Insights

Using this scalable data warehouse on AWS, we can run queries and answer the questions postulated at the beginning of this document:

**What are the months with the most crashes reported?**

According to the data, the month with the overall largest number of reported car crashes is *March* with 148613 incidents, directly followed by *February* and *January* with 148063 and 146521 respectively. This is an intersting outcome, as it is not directly clear why the most incidents occur during the first three months of a year. This could be related to the weather conditions or other factors, such as vacation periods, public holidays, etc.

**What are the weekdays and hours of the day with the most crashes reported?**

The largest number of crashes occurs in the afternoon around 2 p.m. (103193), 4 p.m. (111193), and 5 p.m. (108230). This is most likely related to the afternoon rush hour, when people are driving home from work. Interestingly, there is no clear weekday with a distinct maximum, but a rather uniform distirbution of incidents throughout the week (circa 215000 incidents each day). Naturally, one would expect that during the week, when people are going to work, more car accidents are likely to happen.

**What are the streets where the most crashes occur?**

The three streets with the largest amount of reported car crashes are *Broadway* (13439), *3rd Avenue* (11302), and *Belt Parkway* (10887). This appears reasonable, since Broadway and 3rd Avenue are busy streets in Downtown New York City, while Belt Parkway is a busy highway connecting the city with John F. Kennedy Intl. Airport.

**Can we find relations between the prevalent weather condition and the car crashes?**

The data show that the most accidents happen between -5 °C and +15 °C (595625), while above +15 °C only half as much (191373) and below -5 °C only 10585 incidents are reported. This appears reasonable, since -5 °C to +15 °C is an interval of temperatures often found in winter/fall/spring, where precipitation and freezing temperatures boost the occurence of crashes (slippery roads, etc.). Also, people are more likely to drive during bad/cold weather in winter and fall.

**What are the years with the most injuries reported?**

Sadly, the year 2019 reports the most injuries and fatal casualties of all years with 56962 injuries and 234 lost lives. This corresponds to 0.64 fatalities a day and more than 156 injuries a day. Quite a large amount, even for New York City.

# Outlook to other scenarios

**What if the data were increased by 100x?**

As we are in a highly scalable environment with Amazon Redshift, we could just increase the number of workers and/or upgrade the type of workers used to fit the increased amount of data.

**What if the data pipeline would be run on a daily basis by 7 a.m. every day?**

As we are using Apache Airflow, we can easily schedule the DAG to run on a daily schedule. The SQL insight queries might be included into the DAG if need be.

**What if 100+ people need to access the database?**

Again, AWS Redshift is highly scalable and can handle multiple SQL requests simultaneously. If not, increase the number/type of workers to fit the need.
