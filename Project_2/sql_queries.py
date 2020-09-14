'''
This python script holds all relevant SQL queries to setup and fill the AWS Redshift database.
'''

import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop   = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop    = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop         = "DROP TABLE IF EXISTS songplays"
user_table_drop             = "DROP TABLE IF EXISTS users"
song_table_drop             = "DROP TABLE IF EXISTS songs"
artist_table_drop           = "DROP TABLE IF EXISTS artists"
time_table_drop             = "DROP TABLE IF EXISTS time"

# CREATE STAGING TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR,
        gender          VARCHAR,
        ItemInSession   INT,
        lastName        VARCHAR,
        length          FLOAT,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    DOUBLE PRECISION,
        sessionId       INT,
        song            VARCHAR,
        status          INT,
        ts              TIMESTAMP,
        userAgent       VARCHAR,
        userId          INT
    )
""")

staging_songs_table_create  = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INT
    )
""")

# CREATE FACT TABLE

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id INT NOT NULL IDENTITY(0, 1),
        start_time  TIMESTAMP NOT NULL,
        user_id     INT NOT NULL,
        level       VARCHAR,
        song_id     VARCHAR NOT NULL,
        artist_id   VARCHAR NOT NULL,
        session_id  INT,
        location    VARCHAR,
        user_agent  VARCHAR,
        PRIMARY KEY (songplay_id),
        FOREIGN KEY (start_time)    REFERENCES time (start_time),
        FOREIGN KEY (user_id)       REFERENCES users (user_id),
        FOREIGN KEY (song_id)       REFERENCES songs (song_id),
        FOREIGN KEY (artist_id)     REFERENCES artists (artist_id)
    )
    DISTKEY (song_id)
    SORTKEY (song_id);
""")

# CREATE DIMENSION TABLES

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id     INT NOT NULL,
        first_name  VARCHAR NOT NULL,
        last_name   VARCHAR NOT NULL,
        gender      VARCHAR NOT NULL,
        level       VARCHAR NOT NULL,
        PRIMARY KEY (user_id)
    )
    DISTSTYLE all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id     VARCHAR NOT NULL,
        title       VARCHAR NOT NULL,
        artist_id   VARCHAR NOT NULL,
        year        INT NOT NULL,
        duration    FLOAT NOT NULL,
        PRIMARY KEY (song_id),
        FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
    )
    DISTKEY (song_id)
    SORTKEY (year);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id   VARCHAR NOT NULL,
        name        VARCHAR NOT NULL,
        location    VARCHAR,
        latitude    FLOAT,
        longitude   FLOAT,
        PRIMARY KEY (artist_id)
    )
    DISTSTYLE all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time  TIMESTAMP NOT NULL,
        hour        INT,
        day         INT,
        week        INT,
        month       INT,
        year        INT,
        weekday     INT,
        PRIMARY KEY (start_time)
    )
    DISTSTYLE all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    iam_role '{}'
    REGION 'us-west-2'
    TIMEFORMAT AS 'epochmillisecs'
    FORMAT AS JSON {}
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM', 'ROLE_ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy  = ("""
    COPY staging_songs FROM {}
    iam_role '{}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto'
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM', 'ROLE_ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT DISTINCT
        e.ts        AS start_time,
        e.userId    AS user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId AS session_id,
        e.location,
        e.userAgent AS user_agent
    FROM staging_events e, staging_songs s
    WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users(
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT
        e.userId    AS user_id,
        e.firstName AS first_name,
        e.lastName  AS last_name,
        e.gender,
        e.level
    FROM staging_events e
    WHERE e.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT
        s.song_id,
        s.title,
        s.artist_id,
        s.year,
        s.duration
    FROM staging_songs s;
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT
        s.artist_id,
        s.artist_name       AS name,
        s.artist_location   AS location,
        s.artist_latitude   AS latitude,
        s.artist_longitude  AS longitude
    FROM staging_songs s;
""")

time_table_insert = ("""
    INSERT INTO time(
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT DISTINCT
        sp.start_time,
        EXTRACT(hour FROM start_time)       AS hour,
        EXTRACT(day FROM start_time)        AS day,
        EXTRACT(week FROM start_time)       AS week,
        EXTRACT(month FROM start_time)      AS month,
        EXTRACT(year FROM start_time)       AS year,
        EXTRACT(dayofweek FROM start_time)  AS weekday
    FROM songplays sp;
""")

# QUERY LISTS

create_table_queries    = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries      = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries      = [staging_events_copy, staging_songs_copy]
insert_table_queries    = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
