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
### Create a table containing all of our info
###----------------------------------------------------------------------------------------

query = 'CREATE TABLE IF NOT EXISTS music_library '
query += '(year int, city text, artist_name text, album_name text, PRIMARY KEY (artist_name, album_name))'

try:

    session.execute(query)

except Exception as e:

    print(e)

###----------------------------------------------------------------------------------------
### Insert data into the table
###----------------------------------------------------------------------------------------

query = "INSERT INTO music_library (year, city, artist_name, album_name)"
query = query + " VALUES (%s, %s, %s, %s)"

try:

    session.execute(query, (1970, "Liverpool", "The Beatles", "Let it Be"))
    session.execute(query, (1965, "Oxford", "The Beatles", "Rubber Soul"))
    session.execute(query, (1965, "London", "The Who", "My Generation"))
    session.execute(query, (1966, "Los Angeles", "The Monkees", "The Monkees"))
    session.execute(query, (1970, "San Diego", "The Carpenters", "Close To You"))

except Exception as e:
    print(e)

###----------------------------------------------------------------------------------------
### Query data from table
###----------------------------------------------------------------------------------------

query = "select * from music_library WHERE ARTIST_NAME = 'The Beatles'"

try:

    rows = session.execute(query)

except Exception as e:

    print(e)

for row in rows:

    print (row.year, row.artist_name, row.album_name, row.city)

###----------------------------------------------------------------------------------------
### Drop tables and shut down
###----------------------------------------------------------------------------------------

query = 'DROP TABLE music_library' # On a real system you would never do this query!

try:

    session.execute(query)

except Exception as e:

    print(e)

session.shutdown()
cluster.shutdown()
