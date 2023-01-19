import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist          VARCHAR(MAX),
    auth            VARCHAR,
    first_name      VARCHAR,
    gender          VARCHAR,
    itemInSession   INTEGER,
    last_name       VARCHAR,
    length          FLOAT,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    DECIMAL,
    session_id      INTEGER,
    song            VARCHAR(MAX),
    status          INTEGER,
    ts              BIGINT,
    user_agent      VARCHAR,
    user_id         INTEGER
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs        INTEGER,
    artist_id        VARCHAR,
    artist_latitude  FLOAT,
    artist_longitude FLOAT,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         DECIMAL,
    year             INTEGER
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id      INTEGER  IDENTITY(0,1)  PRIMARY KEY  SORTKEY,
    start_time       TIMESTAMP  NOT NULL,
    user_id          INTEGER    NOT NULL,
    level            VARCHAR    NOT NULL,
    song_id          VARCHAR,
    artist_id        VARCHAR,
    session_id       INTEGER    NOT NULL,
    location         VARCHAR,
    user_agent       VARCHAR
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id          INTEGER  PRIMARY KEY  SORTKEY  DISTKEY,
    first_name       VARCHAR,
    last_name        VARCHAR,
    gender           VARCHAR,
    level            VARCHAR
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id         VARCHAR  PRIMARY KEY,
    title           VARCHAR  NOT NULL,
    artist_id       VARCHAR  NOT NULL DISTKEY,
    year            INTEGER,
    duration        DECIMAL
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id       VARCHAR  PRIMARY KEY  DISTKEY,
    name            VARCHAR  NOT NULL,
    location        VARCHAR,
    lattitude       DECIMAL,
    longitude       DECIMAL
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time      TIMESTAMP  PRIMARY KEY  SORTKEY,
    hour            INTEGER,
    day             INTEGER,
    week            INTEGER,
    month           INTEGER,
    year            INTEGER,
    weekday         INTEGER
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    IAM_ROLE {}
    REGION 'us-west-2'
    FORMAT AS JSON {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    IAM_ROLE {}
    REGION 'us-west-2'
    FORMAT AS JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts * interval '1 second' as start_time, se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
FROM staging_events se
LEFT JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
WHERE se.page = 'NextSong'
;""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE page = 'NextSong' AND user_id IS NOT NULL
;""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL
;""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, lattitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
;""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT 
    DISTINCT TIMESTAMP 'epoch' + ts * interval '1 second' as start_time,
    EXTRACT(hour FROM start_time),
    EXTRACT(day FROM start_time),
    EXTRACT(week FROM start_time),
    EXTRACT(month FROM start_time),
    EXTRACT(year FROM start_time),
    EXTRACT(weekday FROM start_time)
FROM staging_events
WHERE ts IS NOT NULL
;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
