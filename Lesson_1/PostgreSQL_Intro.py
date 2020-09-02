import psycopg2

###----------------------------------------------------------------------------------------
### Connect to local Postgres database and get cursor to the database (autocommit is True)
###----------------------------------------------------------------------------------------

try:

    conn = psycopg2.connect(database = 'postgres', user = 'postgres', password = 'example', host = 'localhost', port = 7000)

except psycop2.Error as e:

    print('Error: Could not connect to Postgres database!')
    print(e)


try:

    cur = conn.cursor()

except psycop2.Error as e:

    print('Error: Could not get cursor to the database!')
    print(e)

conn.set_session(autocommit = True) # Otherwise: conn.commit() after every transaction

###----------------------------------------------------------------------------------------
### Create new database from connected default database
###----------------------------------------------------------------------------------------

try:

    cur.execute('create database udacity')

except psycopg2.Error as e:

    print(e)

###----------------------------------------------------------------------------------------
### Close connection to old database, connect to new one and get cursor to database
###----------------------------------------------------------------------------------------

try:

    conn.close()

except psycopg2.Error as e:
    print(e)

try:

    conn = psycopg2.connect(database = 'udacity', user = 'student', password = 'student')

except psycop2.Error as e:

    print('Error: Could not connect to Postgres database!')
    print(e)


try:

    cur = conn.cursor()

except psycopg2.Error as e:

    print('Error: Could not get cursor to the database!')
    print(e)

conn.set_session(autocommit = True)

###----------------------------------------------------------------------------------------
### Create Music Library of albums
###----------------------------------------------------------------------------------------

# Table Name: music_library
# column 1: Album NAME
# column 2: Artist Name
# column 3: Year

try:

    cur.execute('CREATE TABLE IF NOT EXISTS music_library (album_name varchar, artist_name varchar, year int)')

except psycopg2.Error as e:

    print('Error: Issue creating table!')
    print(e)

try:

    cur.execute('select count(*) from music_library')

except psycopg2.Error as e:

    print('Error: Issue creating table!')
    print(e)

print(cur.fetchall())

###----------------------------------------------------------------------------------------
### Fill in two rows with data
###----------------------------------------------------------------------------------------

try:

    cur.execute('INSERT INTO music_library (album_name, artist_name, year) \
                    VALUES (%s, %s, %s)', \
                    ('Let It Be', 'The Beatles', 1970))

except psycopg2.Error as e:

    print('Error: Issue inserting rows!')
    print(e)

try:

    cur.execute('INSERT INTO music_library (album_name, artist_name, year) \
                    VALUES (%s, %s, %s)', \
                    ('Rubber Soul', 'The Beatles', 1965))

except psycopg2.Error as e:

    print('Error: Issue inserting rows!')
    print(e)

###----------------------------------------------------------------------------------------
### Validate that data is stored in database
###----------------------------------------------------------------------------------------

try:

    cur.execute('SELECT * FROM music_library;')

except psycopg2.Error as e:

    print('Error: Issue select *!')
    print(e)

row = cur.fetchone()

while row:

    print(row)
    row = cur.fetchone()

###----------------------------------------------------------------------------------------
### Drop the table and close cursor and connection
###----------------------------------------------------------------------------------------

try:

    cur.execute('DROP table music_library')

except psycopg2.Error as e:

    print('Error: Issue dropping table!')
    print(e)

cur.close()
conn.close()
