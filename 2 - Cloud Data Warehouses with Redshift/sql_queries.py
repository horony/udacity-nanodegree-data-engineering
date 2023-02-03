import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

config_arn = config.get("IAM_ROLE", "ARN")
config_log_data = config.get("S3", "LOG_DATA")
config_log_jsonpath = config.get("S3", "LOG_JSONPATH")
config_song_data = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_song_data"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_log_data"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS stage_log_data (
        artist varchar
        , auth varchar
        , firstName varchar
        , gender varchar
        , itemInSession integer
        , lastName varchar
        , length float
        , level varchar
        , method varchar
        , page varchar
        , registration float
        , sessionId integer
        , song varchar
        , status integer
        , ts bigint
        , userAgent varchar
        , userId bigint
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stage_song_data (
        num_songs bigint
        , artist_id varchar
        , artist_latitude float
        , artist_longitude float
        , artist_location varchar
        , artist_name varchar
        , song_id varchar
        , title varchar
        , duration float
        , year integer        
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id bigint primary key
        , start_time timestamp
        , user_id bigint
        , level varchar
        , song_id varchar
        , artist_id varchar
        , session_id bigint
        , location varchar
        , user_agent varchar
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id bigint primary key
        , first_name varchar
        , last_name varchar
        , gender varchar
        , level varchar
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar primary key
        , title varchar
        , artist_id varchar
        , year int
        , duration float
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar primary key
        , name varchar
        , location varchar
        , latitude float
        , longitude float
    )
""")

time_table_create = ("""
    CREATE TABLE time as (
        start_time timestamp primary key
        , hour integer
        , day integer
        , week integer
        , month integer
        , year integer
        , weekday integer
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy stage_log_data
    from {0}
    iam_role {1}
    json {2};
""").format(config_log_data, config_arn, config_log_jsonpath)

staging_songs_copy = ("""
    copy stage_song_data
    from {0}
    iam_role {1}
    json 'auto';
""").format(config_song_data, config_arn)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays
    
    SELECT  row_number() over() as songplay_id 
            , timestamp 'epoch' + log.ts/1000 * interval '1 second' as start_time
            , log.userId as user_id 
            , log.level 
            , log.song_id 
            , song.artist_id
            , log.sessionId as session_id
            , log.location
            , log.userAgent as user_agent 
            
    FROM stage_log_data log
    
    LEFT JOIN stage_song_data song
        ON  log.artist = song.artist_name
            AND log.song = song.title
            AND round(log.lenght,2) = round(song.duration,2)
    
    WHERE log.page = 'NextSong'
    ORDER BY log.ts asc, log.sessionId asc
    ;
""")

user_table_insert = ("""
    INSERT INTO users
    
    SELECT  DISTINCT log.userId 
            , log.firstName 
            , log.lastName 
            , log.gender 
            , log.level 
        
    FROM stage_log_data log
    WHERE log.page = 'NextSong' and log.userId is not null
    ;
""")

song_table_insert = ("""
    INSERT INTO songs
    
    SELECT DISTINCT song.song_id 
        , song.title
        , song.artist_id
        , song.year
        , song.duration 
        
    FROM stage_song_data song
    WHERE song.song_id is not null
    ;
""")

artist_table_insert = ("""
    INSERT INTO artists
    
    SELECT DISTINCT song.artist_id 
        , song.artist_name
        , song.artist_location
        , song.artist_latitude
        , song.artist_longitude
        
    FROM stage_song_data song
    WHERE song.artist_id is not null
    ;
""")

time_table_insert = ("""
    INSERT INTO time
    
    SELECT  DISTINCT timestamp 'epoch' + log.ts/1000 * interval '1 second' as start_time
            , EXTRACT(hour from start_time)
            , EXTRACT(day from start_time)
            , EXTRACT(week from start_time)
            , EXTRACT(month from start_time)
            , EXTRACT(year from start_time)
            , EXTRACT(weekday from start_time)
        
    FROM stage_log_data log
    WHERE log.page = 'NextSong' and log.ts is not null
    ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]