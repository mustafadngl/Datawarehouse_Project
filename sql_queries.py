import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events (
                                artist TEXT,
                                auth TEXT,
                                first_name TEXT,
                                gender VARCHAR (1),
                                item_in_session INT,
                                last_name TEXT,
                                length FLOAT,
                                level TEXT,
                                location TEXT,
                                method VARCHAR (3),
                                page TEXT,
                                registration FLOAT,
                                session_id INT,
                                song TEXT,
                                status INT,
                                ts BIGINT,
                                user_agent TEXT,
                                user_id INT)
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs (
                                 num_songs INT,
                                 artist_id TEXT,
                                 artist_latitude TEXT,
                                 artist_longitude TEXT,
                                 artist_location TEXT,
                                 artist_name TEXT,
                                 song_id TEXT,
                                 title TEXT,
                                 duration FLOAT,
                                 year INT)
            
""")

songplay_table_create = ("""CREATE TABLE songplays (
                            songplay_id IDENTITY(0,1),
                            start_time BIGINT NOT NULL SORTKEY,
                            user_id INT NOT NULL DISTKEY,
                            level TEXT,
                            song_id TEXT, 
                            artist_id TEXT,    
                            session_id INT,   
                            location TEXT,   
                            user_agent TEXT,   
                            PRIMARY KEY (songplay_id))                                                      
""")

user_table_create = ("""CREATE TABLE users (
                        user_id TEXT PRIMARY KEY SORTKEY, 
                        first_name TEXT NOT NULL,   
                        last_name TEXT NOT NULL, 
                        gender INT NOT NULL DISTKEY, 
                        level DECIMAL(8,5) NOT NULL)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                        song_id TEXT PRIMARY KEY SORTKEY,
                        title TEXT NOT NULL,
                        artist_id TEXT NOT NULL DISTKEY,
                        year INT NOT NULL , 
                        duration DECIMAL(8,5) NOT NULL)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                        artist_id TEXT PRIMARY KEY SORTKEY,
                        name TEXT NOT NULL,
                        location TEXT, 
                        latitude DECIMAL, 
                        longitude DECIMAL)
""")

time_table_create = ("""CREATE TABLE time  (
                    start_time TIMESTAMP PRIMARY KEY SORTKEY, 
                    hour INT,    
                    day INT NOT NULL, 
                    week INT NOT NULL, 
                    month INT NOT NULL, 
                    year INT NOT NULL DISTKEY,
                    weekday TEXT NOT NULL)
""")

# STAGING TABLES

staging_events_copy = ("""COPY {}
                          FROM {}
                          CREDENTIALS {}
                          JSON{}
""").format('staging_event',
            config('S3','LOG_DATA'),
            config('IAM_ROLE','ARN'),
            config('S3','LOG_JSONPATH'),
           )

staging_songs_copy = ("""COPY {}
                         FROM {}
                         CREDENTIALS {}
                         JSON{}
""").format('staging_songs',
            config.get('S3','SONG_DATA'),
            config.get('IAM_ROLE','ARN'),
            'auto'
           )

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (songplay_id,start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
                            SELECT DISTINCT TIMESTAMP 'epoch' +(e.ts/1000* INTERVAL '1 second') AS start_time,
                            e.user_id,
                            e.level,
                            s.song_id,
                            s.artist_id,
                            e.session_id,
                            e.location,
                            e.user_agent,
                            FROM staging_events e
                            JOIN staging_songs s 
                            ON e.song=s.title AND e.artist=s.first_name AND e.length = s.duration AND e.artist=s.title
                            WHERE page='NextSong' 

""")

user_table_insert = ("""INSERT INTO users (user_id,first_name,last_name,gender,year)
                        SELECT DISTINCT e.user_id PRIMARY KEY,
                        e.first_name,
                        e.last_name,
                        e.gender,
                        e.level                       
                        FROM staging_events e
                        WHERE e.user_id NOT NULL AND page='NextSong'
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT s.song_id PRIMARY KEY,
                        s.title,
                        s.artist_id,
                        s.year,
                        s.duration
                        FROM staging_songs s
                        WHERE s.song_id NOT NULL
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude)
                          SELECT DISTINCT s.artist_id PRIMARY KEY,
                          s.artist_name,
                          s.artist_location,
                          s.artist_lattitude,
                          s.artist_longitude
                          FROM staging_songs s
                          WHERE s.artist_id NOT NULL
                          
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT start_time PRIMARY KEY,
                        EXTRACT (hour FROM start_time),
                        EXTRACT (day FROM start_time),
                        EXTRACT (week FROM start_time),
                        EXTRACT (month FROM start_time),
                        EXTRACT (year FROM start_time),
                        EXTRACT (dow FROM start_time)
                        FROM songplays
                        WHERE start_time NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
