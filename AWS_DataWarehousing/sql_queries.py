import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events cascade;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
                                CREATE TABLE IF NOT EXISTS staging_events
                                (
                                artist          VARCHAR,
                                auth            VARCHAR, 
                                firstName       VARCHAR,
                                gender          VARCHAR,   
                                itemInSession   INTEGER,
                                lastName        VARCHAR,
                                length          FLOAT,
                                level           VARCHAR, 
                                location        VARCHAR,
                                method          VARCHAR,
                                page            VARCHAR,
                                registration    BIGINT,
                                sessionId       INTEGER,
                                song            VARCHAR,
                                status          INTEGER,
                                ts              TIMESTAMP,
                                userAgent       VARCHAR,
                                userId          INTEGER
                                );
                                """)

staging_songs_table_create = ("""
                                CREATE TABLE IF NOT EXISTS staging_songs
                                (
                                song_id            VARCHAR,
                                num_songs          INTEGER,
                                title              VARCHAR,
                                artist_name        VARCHAR,
                                artist_latitude    FLOAT,
                                year               INTEGER,
                                duration           FLOAT,
                                artist_id          VARCHAR,
                                artist_longitude   FLOAT,
                                artist_location    VARCHAR
                                );
                                """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
  songplay_id int IDENTITY(0,1) not null	sortkey distkey,
  start_time        	 timestamp    	not null,
  user_id        	integer,
  level    	varchar      not null,
  song_id      	varchar      not null,
  artist_id       	varchar 	not null,
  session_id        	integer 	not null,
  location        	varchar     ,
  user_agent   	TEXT     
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS USERS
                            (
                             user_id int     	sortkey,
                             first_name varchar,
                             last_name varchar,
                             gender varchar,
                             level varchar
                            )
                        diststyle all;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS SONGS
                            (
                             song_id varchar     	not null	sortkey,
                             title text,
                             artist_id varchar NOT NULL,
                             year integer,
                             duration float
                             )
                             diststyle all;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
                            (
                             artist_id varchar     	not null 	sortkey,
                             artist_name varchar,
                             artist_location varchar,
                             artist_latitude float,
                             artist_longitude float
                             )
                             diststyle all;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
                            (
                             start_time timestamp    	not null	sortkey,
                             hour int,
                             day int,
                             week int,
                             month int,
                             year int,
                             weekday int
                             )
                             diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
                        COPY staging_events FROM {}
                            CREDENTIALS 'aws_iam_role={}'
                            COMPUPDATE OFF region 'us-west-2'
                            TIMEFORMAT as 'epochmillisecs'
                            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
                            FORMAT AS JSON {};
                            """).format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
                        COPY staging_songs FROM {}
                            CREDENTIALS 'aws_iam_role={}'
                            COMPUPDATE OFF region 'us-west-2'
                            FORMAT AS JSON 'auto' 
                            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
                        """).format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays 
                            (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT DISTINCT se.ts, se.userid, se.level, ss.song_id, ss.artist_id, se.sessionid, se.location, se.useragent
                            FROM staging_events se
                            JOIN staging_songs ss
                            ON se.artist = ss.artist_name;                    
""")

user_table_insert = ("""INSERT INTO users 
                            (user_id, first_name, last_name, gender, level)
                            SELECT DISTINCT userid, firstname,lastname, gender, level
                            FROM staging_events;
""")                         

song_table_insert = ("""INSERT INTO songs 
                            (song_id, title, artist_id, year, duration)
                            SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
                            FROM staging_events se
                            JOIN staging_songs ss
                            ON se.artist = ss.artist_name;
""")

artist_table_insert = ("""INSERT INTO artists 
                            (artist_id, artist_name, artist_location,artist_latitude, artist_longitude)
                            SELECT DISTINCT ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
                            FROM staging_songs ss;
""")

time_table_insert = ("""INSERT INTO time 
                            (start_time, hour, day, week, month,year, weekday)
                            SELECT ts, DATE_PART(hour,ts),DATE_PART(day,ts),DATE_PART(day,ts),DATE_PART(month,ts),DATE_PART(year,ts),DATE_PART(dow,ts)
                            FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

