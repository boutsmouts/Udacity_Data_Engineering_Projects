import cassandra
from cassandra.cluster import Cluster

###----------------------------------------------------------------------------------------
### Connect to the cluster on localhost under port 9042 (default - specified in YAML for docker)
###----------------------------------------------------------------------------------------

try:

    cluster = Cluster(['localhost'])
    session = cluster.connect()

except Exception as e:

    print(e)

###----------------------------------------------------------------------------------------
### Create a keypsace to do all the work in (replication_factor gives number of data copies - on 1 node only 1)
###----------------------------------------------------------------------------------------

try:

    session.execute('''
                    CREATE KEYSPACE IF NOT EXISTS udacity
                    WITH REPLICATION =
                    { 'class': 'SimpleStrategy', 'replication_factor': 1}'''
                    )

except Exception as e:

    print(e)

###----------------------------------------------------------------------------------------
### Connect to the keyspace
###----------------------------------------------------------------------------------------

try:

    session.set_keyspace('udacity')

except Exception as e:

    print(e)

###----------------------------------------------------------------------------------------
### Create three different tables made for three different queries
###----------------------------------------------------------------------------------------

query_0 = 'CREATE TABLE IF NOT EXISTS music_library '
query_0 += '(year int, artist_name text, album_name text, PRIMARY KEY (year, artist_name))'

query_1 = 'CREATE TABLE IF NOT EXISTS artist_library '
query_1 += '(artist_name text, album_name text, year int, PRIMARY KEY (artist_name, year))'

query_2 = 'CREATE TABLE IF NOT EXISTS album_library '
query_2 += '(album_name text, artist_name text, year int, PRIMARY KEY (album_name, year))'

try:

    session.execute(query_0)
    session.execute(query_1)
    session.execute(query_2)

except Exception as e:

    print(e)

###----------------------------------------------------------------------------------------
### Insert data into the three tables
###----------------------------------------------------------------------------------------

query_0 = 'INSERT INTO music_library (year, artist_name, album_name)'
query_0 += ' VALUES (%s, %s, %s)'

query_1 = 'INSERT INTO artist_library (artist_name, album_name, year)'
query_1 += ' VALUES (%s, %s, %s)'

query_2 = 'INSERT INTO album_library (album_name, artist_name, year)'
query_2 += ' VALUES (%s, %s, %s)'

try:

    session.execute(query_0, (1970, "The Beatles", "Let it Be"))
    session.execute(query_0, (1965, "The Beatles", "Rubber Soul"))
    session.execute(query_0, (1966, "The Monkees", "The Monkees"))
    session.execute(query_0, (1970, "The Carpenters", "Close To You"))

    session.execute(query_1, ("The Beatles", "Let it Be", 1970))
    session.execute(query_1, ("The Beatles", "Rubber Soul", 1965))
    session.execute(query_1, ("The Monkees", "The Monkees", 1966))
    session.execute(query_1, ("The Carpenters", "Close To You", 1970))

    session.execute(query_2, ("Let it Be", "The Beatles", 1970))
    session.execute(query_2, ("Rubber Soul", "The Beatles", 1965))
    session.execute(query_2, ("The Monkees", "The Monkees", 1966))
    session.execute(query_2, ("Close To You", "The Carpenters", 1970))

except Exception as e:

    print(e)

###----------------------------------------------------------------------------------------
### Perform three queries to retrieve desired data from the tables
###----------------------------------------------------------------------------------------

query = "select * from music_library WHERE YEAR = 1970"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print (row.year, row.artist_name, row.album_name)



query = "select * from artist_library WHERE ARTIST_NAME = 'The Monkees'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print (row.year, row.artist_name, row.album_name)



query = "select * from album_library WHERE ALBUM_NAME = 'Close To You'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print (row.year, row.artist_name, row.album_name)


session.shutdown()
cluster.shutdown()
