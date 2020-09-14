'''
This small script runs two sample queries to

    retrieve the number of rows in table songs
    retrieve the top ten artists of table songplays
'''

import configparser
import psycopg2
import boto3

'''
Establish connection to Redshift database
'''

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cur = conn.cursor()

'''
Number of rows in table songs
'''

print('Number of rows in table songs:')
cur.execute('''SELECT count(*) FROM songs''')
conn.commit()
cur.fetchall()[0][0]

print('')

'''
Top ten artists of table songplays
'''

print('The ten most listened artists are:')
cur.execute('''SELECT artists.name, COUNT(songplay_id) AS amount FROM songplays
                JOIN songs ON (songplays.song_id = songs.song_id)
                JOIN artists ON (songplays.artist_id = artists.artist_id)
                GROUP BY (artists.name)
                ORDER BY amount DESC
                LIMIT 10''')
conn.commit()

row = cur.fetchone()

while row:
    print(row)
    row = cur.fetchone()

'''
Close connection to Redshift database
'''

cur.close()
conn.close()
