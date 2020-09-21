import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']     = config.get('AWS', 'KEY')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'SECRET')


def create_spark_session():

    '''
    FUNCTION:   create_spark_session
    PURPOSE:    Creates a new Apache Spark session including the hadoop-aws framework
    INPUT:      None
    OUTPUT:     spark:  the desired spark session
    '''

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

spark = create_spark_session()

def process_song_data(spark, input_data, output_data):

    '''
    FUNCTION:   process_song_data
    PURPOSE:    Uses Apache Spark to load song data from json files, processes them in tables, and writes
                tables to .parquet files
    INPUT:      spark:          an existing Spark session
                input_data:     the parent file path of the input song data
                output_data:    the parent file path for the output
    OUTPUT:     No explicit return, prints to console
    '''
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration') \
        .dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + 'songs/', mode = 'overwrite', partitionBy = ['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
        .withColumnRenamed('artist_name', 'name') \
        .withColumnRenamed('artist_location', 'location') \
        .withColumnRenamed('artist_latitude', 'latitude') \
        .withColumnRenamed('artist_longitude', 'longitude') \
        .dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/', mode = 'overwrite')


def process_log_data(spark, input_data, output_data):

    '''
    FUNCTION:   process_log_data
    PURPOSE:    Uses Apache Spark to load log data from json files, processes them in tables, and writes
                tables to .parquet files
    INPUT:      spark:          an existing Spark session
                input_data:     the parent file path of the input log data
                output_data:    the parent file path for the output
    OUTPUT:     No explicit return, prints to console
    '''

    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level') \
        .withColumnRenamed('userId', 'user_id') \
        .withColumnRenamed('firstName', 'first_name') \
        .withColumnRenamed('lastName', 'last_name') \
        .dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/', mode = 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x) / 1000.), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))

    # extract columns to create time table
    time_table = df.select('start_time') \
        .withColumn('hour', hour('start_time')) \
        .withColumn('day', dayofmonth('start_time')) \
        .withColumn('week', weekofyear('start_time')) \
        .withColumn('month', month('start_time')) \
        .withColumn('year', year('start_time')) \
        .withColumn('weekday', dayofweek('start_time')) \
        .dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time/', mode = 'overwrite', partitionBy = ['year', 'month'])

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'

    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, df.song == song_df.title, 'inner') \
        .select('start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent') \
        .withColumnRenamed('userId', 'user_id') \
        .withColumnRenamed('sessionId', 'session_id') \
        .withColumnRenamed('userAgent', 'user_agent') \
        .withColumn('year', year('start_time')) \
        .withColumn('month', month('start_time')) \
        .withColumn('songplay_id', monotonically_increasing_id()) \
        .dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays/', mode = 'overwrite', partitionBy = ['year', 'month'])

def main():

    '''
    FUNCTION:   main
    PURPOSE:    Parses file paths to the functions specified and runs the ELT process
    INPUT:      None
    OUTPUT:     No explicit return, prints to console
    '''

    print('Creating Spark session...')

    spark = create_spark_session()

    print('Spark session successfully created.')

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://mh-dend-udacity/data_lake_project/output/"

    print('Processing song data...')

    process_song_data(spark, input_data, output_data)

    print('Song data successfully processed.')
    print('Processing log data...')

    process_log_data(spark, input_data, output_data)

    print('Log data successfully processed. Stopping spark session.')

    spark.stop()


if __name__ == "__main__":
    main()
