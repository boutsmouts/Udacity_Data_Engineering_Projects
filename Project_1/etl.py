import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):

    '''
    FUNCTION:   process_log_file
    PURPOSE:    Process a given log file with pandas and write data into artists and songs dimension tables using psycopg2
    INPUT:      cur:        cursor on connected PostgreSQL database
                filepath:   path to specific log file
    OUTPUT:     No explicit return, function calls specified SQL queries (see sql_queries.py)
    '''

    # open song file
    df = pd.DataFrame([pd.read_json(filepath, typ = 'series')])

    # insert artist record
    artist_data = ((df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values).tolist())[0]
    cur.execute(artist_table_insert, artist_data)

    # Note: I've switched the order of the insertion since the table songs refers to artist_id from table artists.

    # insert song record
    song_data = ((df[['song_id', 'title', 'artist_id', 'year', 'duration']].values).tolist())[0]
    cur.execute(song_table_insert, song_data)


def process_log_file(cur, filepath):

    '''
    FUNCTION:   process_log_file
    PURPOSE:    Process a given log file with pandas and write data into users and time dimension tables using psycopg2
    INPUT:      cur:        cursor on connected PostgreSQL database
                filepath:   path to specific log file
    OUTPUT:     No explicit return, function calls specified SQL queries (see sql_queries.py)
    '''

    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit = 'ms')

    # insert time data records
    time_data = []

    for row in t:
        time_data.append([row, row.hour, row.day, row.weekofyear, row.month, row.year, row.dayofweek])

    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row['ts'], unit = 'ms'), row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):

    '''
    FUNCTION:   process_data
    PURPOSE:    Walk down a specified file path, grab all matching .json files, and process them using a passed function
    INPUT:      cur:        cursor on connected PostgreSQL database
                conn:       connection to PostgreSQL database
                filepath:   path to walk down
                func:       function used to process all found files along the file path tree
    OUTPUT:     No explicit return, function calls specified SQL queries (see sql_queries.py)
    '''

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():

    '''
    FUNCTION:   main
    PURPOSE:    Main driving function to fill the desired PostgreSQL database
    INPUT:      None
    OUTPUT:     No explicit return, function calls specified SQL queries (see sql_queries.py)
    '''

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

if __name__ == "__main__":
    main()
