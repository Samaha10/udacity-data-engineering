import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')

LOG_DATA=config.get('S3','LOG_DATA')
SONG_DATA=config.get('S3','SONG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

# registration 12-digit registration codes, not supposed to perform numerical operations on them
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events_table(  
                                artist VARCHAR, 
                                auth VARCHAR(30), 
                                firstName VARCHAR, 
                                gender VARCHAR(20),
                                itemInSession INT, 
                                lastName VARCHAR, 
                                length NUMERIC, 
                                level VARCHAR(20),
                                location VARCHAR, 
                                method VARCHAR(10), 
                                page VARCHAR(20), 
                                registration VARCHAR, 
                                sessionId BIGINT, 
                                song VARCHAR, 
                                status INT, 
                                ts BIGINT, 
                                userAgent VARCHAR, 
                                userId INT)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table (num_songs INT, 
                                artist_id VARCHAR, 
                                artist_latitude DOUBLE PRECISION, 
                                artist_longitude DOUBLE PRECISION, 
                                artist_location VARCHAR, 
                                artist_name VARCHAR, 
                                song_id VARCHAR, 
                                title VARCHAR, 
                                duration NUMERIC, 
                                year INT)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table (
                            songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
                            start_time TIMESTAMP NOT NULL  , 
                            user_id INT NOT NULL REFERENCES user_table(user_id), 
                            level VARCHAR(20), 
                            song_id VARCHAR REFERENCES song_table(song_id), 
                            artist_id VARCHAR REFERENCES artist_table(artist_id), 
                            session_id BIGINT, 
                            location VARCHAR, 
                            user_agent VARCHAR);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table (
                        user_id INT NOT NULL PRIMARY KEY, 
                        first_name VARCHAR, 
                        last_name VARCHAR, 
                        gender VARCHAR(20), 
                        level VARCHAR(20));
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table (
                        song_id VARCHAR NOT NULL PRIMARY KEY, 
                        title VARCHAR NOT NULL, 
                        artist_id VARCHAR, 
                        year INT, 
                        duration NUMERIC NOT NULL);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table (
                            artist_id VARCHAR NOT NULL , 
                            name VARCHAR NOT NULL, 
                            location VARCHAR, 
                            latitude DOUBLE PRECISION, 
                            longitude DOUBLE PRECISION, 
                            PRIMARY KEY(artist_id));
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table (
                        start_time TIMESTAMP PRIMARY KEY, 
                        hour INT, 
                        day INT, 
                        week INT, 
                        month INT, 
                        year INT, 
                        weekday INT);
""")

# STAGING TABLES

staging_events_copy = ("""     
copy staging_events_table from {} 
credentials 'aws_iam_role={}'
region 'us-west-2'
json {};
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs_table from {} 
credentials 'aws_iam_role={}'
 region 'us-west-2'
json 'auto ignorecase';
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay_table (start_time, 
    user_id, level, 
    song_id, artist_id, 
    session_id, location, user_agent) 
    SELECT DISTINCT (TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second'), e.userId, e.level, 
    s.song_id, s.artist_id, 
    e.sessionId, e.location, e.userAgent
    FROM staging_events_table e, staging_songs_table s 
    WHERE e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration AND  e.page = 'NextSong' ; 
""")


user_table_insert = ("""INSERT INTO user_table (user_id, first_name, 
    last_name, gender, level) SELECT DISTINCT userId, firstName, 
    lastName, gender, level  
    FROM staging_events_table
    WHERE page = 'NextSong'; 
""")

song_table_insert = ("""INSERT INTO song_table (song_id , title, artist_id, 
    year, duration)  SELECT DISTINCT song_id, title, 
    artist_id, year,  duration   FROM staging_songs_table;
""")

artist_table_insert = ("""INSERT INTO artist_table (artist_id ,
    name, location, 
    latitude, longitude) 
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude,  artist_longitude   
    FROM staging_songs_table;
""")
# TO_TIMESTAMP(e.ts / 1000)   ,  to_timestamp(bigint) not supported in redshift

time_table_insert = ("""INSERT INTO time_table (start_time, hour,
                day, week, month, 
                year, weekday) SELECT DISTINCT start.ts, 
                EXTRACT (HOUR FROM start.ts), 
                EXTRACT (DAY FROM start.ts), 
                EXTRACT (WEEK FROM start.ts), 
                EXTRACT (MONTH FROM start.ts),
                EXTRACT (YEAR FROM start.ts), 
                EXTRACT (DOW FROM start.ts)
                FROM (SELECT (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') as ts
                FROM staging_events_table WHERE page = 'NextSong') as start
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]


