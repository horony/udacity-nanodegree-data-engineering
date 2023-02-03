# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id integer PRIMARY KEY
        , first_name varchar
        , last_name varchar
        , gender varchar
        , level varchar
        )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY
        , name varchar
        , location varchar
        , latitude float
        , longitude float
        )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY
        , title varchar
        , artist_id varchar REFERENCES artists (artist_id)
        , year integer
        , duration float
        )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        starttime timestamp PRIMARY KEY
        , hour integer
        , day integer
        , week integer
        , month integer
        , year integer
        , weekday integer
        )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id integer PRIMARY KEY
        , start_time timestamp REFERENCES time (starttime)
        , user_id integer REFERENCES users (user_id)
        , level varchar
        , song_id varchar REFERENCES songs (song_id)
        , artist_id varchar REFERENCES artists (artist_id)
        , session_id varchar
        , location varchar
        , user_agent varchar
        )
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (songplay_id) DO NOTHING
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE 
        SET first_name = excluded.first_name, 
            last_name = excluded.last_name,
            gender = excluded.gender,
            level = excluded.level
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO UPDATE 
        SET title = excluded.title, 
            artist_id = excluded.artist_id,
            year = excluded.year,
            duration = excluded.duration
""")
       
artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO UPDATE 
        SET name = excluded.name, 
            location = excluded.location,
            latitude = excluded.latitude,
            longitude = excluded.longitude
""")

time_table_insert = ("""
    INSERT INTO time (starttime, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (starttime) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT s.song_id, s.artist_id
    FROM   songs s
    LEFT JOIN artists a
        ON s.artist_id = a.artist_id
    WHERE   a.name = %s 
            AND s.title = %s 
            AND round(CAST(s.duration as NUMERIC),0) = round(CAST(%s as NUMERIC),0)
""")

# QUERY LISTS

create_table_queries = [artist_table_create, song_table_create, time_table_create, user_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]