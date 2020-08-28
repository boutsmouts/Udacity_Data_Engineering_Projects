import cassandra
from cassandra.cluster import Cluster

###----------------------------------------------------------------------------------------
### Connect to the cluster on localhost under port 6000 (specified in YAML for docker)
###----------------------------------------------------------------------------------------

try:

    cluster = Cluster(['localhost'], port = 6000)
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
### Create music_library - the album year will be the partition key
### the artist name will be the clusterin column
### (remember: there are no duplicates in Cassandra!)
###----------------------------------------------------------------------------------------

query = 'CREATE TABLE IF NOT EXISTS music_library '

query += '(year int, artist_name text, album_name text, PRIMARY KEY (year, artist_name))'

try:

    session.execute(query)

except Exception as e:

    print(e)

###----------------------------------------------------------------------------------------
### Check that table was created
###----------------------------------------------------------------------------------------

query = 'select count(*) from music_library'

try:

    count = session.execute(query)

except Exception as e:

    print(e)

print(count.one())

###----------------------------------------------------------------------------------------
### Insert two rows of data into the table
###----------------------------------------------------------------------------------------

query = 'INSERT INTO music_library (year, artist_name, album_name)'
query += ' VALUES (%s, %s, %s)'

try:

    session.execute(query, (1970, 'The Beatles', 'Let It Be'))

except Exception as e:

    print(e)

try:

    session.execute(query, (1965, 'The Beatles', 'Rubber Soul'))

except Exception as e:

    print(e)

###----------------------------------------------------------------------------------------
### Validate that data is in the table
###----------------------------------------------------------------------------------------

query = 'SELECT * FROM music_library' # On a real system you would never do this query!

try:

    rows = session.execute(query)

except Exception as e:

    print(e)

for row in rows:

    print(row.year, row.album_name, row.artist_name)

###----------------------------------------------------------------------------------------
### Use a WHERE-query for year == 1970
###----------------------------------------------------------------------------------------

query = 'SELECT * FROM music_library WHERE YEAR = 1970' # On a real system you would never do this query!

try:

    rows = session.execute(query)

except Exception as e:

    print(e)

for row in rows:

    print(row.year, row.album_name, row.artist_name)

###----------------------------------------------------------------------------------------
### Drop the table, close session and shutdown cluster
###----------------------------------------------------------------------------------------

query = 'DROP TABLE music_library' # On a real system you would never do this query!

try:

    rows = session.execute(query)

except Exception as e:

    print(e)

session.shutdown()
cluster.shutdown()
