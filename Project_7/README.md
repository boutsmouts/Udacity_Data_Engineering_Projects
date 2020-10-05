# Project 7: Capstone Project

# Introduction

New York City is one of the busiest cities in the world with circa 8.5 million citizens and almost 20 million citizens in its metropolitan area (see [Wikipedia](https://de.wikipedia.org/wiki/New_York_City)). This implies that the current traffic situation within the city is quite heavy, which inevitably leads to traffic accidents all over the city. It is likely that that the occurence of these car crashes and accidents accumulates around main traffic axes of the city and follows certain temporal patterns. Additionally, the weather conditions might also have an influence on these accidents.

The focus of this Capstone Project is to build a cloud data warehouse in Amazon Redshift using data on S3, fill it with suitable datasets on a plannable time scale using Apache Airflow and run SQL queries on AWS Redshift to answer the following questions:
  - What are the months with the most crashes reported?
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
