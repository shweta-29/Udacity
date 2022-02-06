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

staging_events_table_create= ("""CREATE TABLE staging_events (
                                 artist TEXT PRIMARY KEY, 
                                 firstname varchar,
                                 gender varchar,
                                 iteminsession int,
                                 lastname varchar, 
                                 length varchar,
                                 level varchar,
                                 location TEXT,
                                 page varchar,
                                 sessionid int,
                                 song text,
                                 ts int,
                                 useragent text,
                                 userid int
);
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs (
                                 num_songs integer PRIMARY KEY, 
                                 artist_id varchar NOT NULL, 
                                 artist_latitude float,
                                 artist_longitude float,
                                 artist_location varchar,
                                 artist_name varchar, 
                                 song_id varchar NOT NULL,
                                 title TEXT,
                                 duration float,
                                 year int
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
  songplay_id int IDENTITY(0,1) not null	sortkey distkey,
  start_time        	 timestamp    	not null,
  user_id        	integer      not null,
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
                             user_id int     	not null	sortkey,
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
                             year int,
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

staging_events_copy = ("""copy staging_events from '{}'
    credentials 'aws_iam_role={}'
    json 'auto' compupdate on region 'us-west-2';
""".format((LOG_DATA,ARN))
        
staging_songs_copy = ("""copy staging_songs
                        from '{}'credentials 'aws_iam_role={}' json 'auto'
                        compupdate on region 'us-west-2';""").format(SONG_DATA,ARN)                 


# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

