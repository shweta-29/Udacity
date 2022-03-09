import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - Read song_data json into a dataframe
    - From dataframe, Extract data into songs, artist table 
    - Write all dataframe into parquet files
    """
    # get filepath to song data file s3a://udacity-dend/
    song_data = input_data + './data/song_data/*.json'
    
    # read song data file
    df = spark.read.json(song_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.parquet(output_data+'./songs_table/', partitionBy=["year","artist_id"], mode='overwrite') 

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude') \
                        .withColumnRenamed('artist_name', 'artist') \
                        .withColumnRenamed('artist_location', 'location') \
                        .withColumnRenamed('artist_latitude', 'latitude') \
                        .withColumnRenamed('artist_longitude', 'longitude').dropDuplicates()

    # write artists table to parquet files
    artists_table = artists_table.write.parquet(output_data+'./artists_table/', mode='overwrite' )

def process_log_data(spark, input_data, output_data):
    """
        - Read log_data json into a dataframe
        - From dataframe, Extract data into users, songplays table 
        - Write all dataframe into parquet files
    """
    # get filepath to log data file s3a://udacity-dend/
    log_data = input_data + './data/log_data/*.json'

    # read log data file
    df = spark.read.json(log_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table =  df.select(
                            ['userId', 'firstName', 'lastName', 'gender', 'level']) \
                            .withColumnRenamed('userId', 'user_id')\
                            .withColumnRenamed('firstName', 'first_name')\
                            .withColumnRenamed('lastName', 'last_name').dropDuplicates()
    
    # write users table to parquet files
    users_table = users_table.write.parquet(output_data+'./users_table/', mode='overwrite' )

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
#     get_datetime = udf()
#     df = 

    # extract columns to create time table
    time_table = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()

    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.parquet(output_data+'./time_table/', partitionBy=["year","month"], mode='overwrite' )

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + './data/song_data/*.json')
    
    song_df.createOrReplaceTempView('songs') 
    log_df = df.withColumn('songplay_id', F.monotonically_increasing_id())
    log_df.createOrReplaceTempView('logs')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table =  spark.sql("""
                                SELECT
                                    l.songplay_id,
                                    l.datetime as start_time,
                                    year(l.datetime) as year,
                                    month(l.datetime) as month,
                                    l.userId as user_id,
                                    l.level,
                                    s.song_id,
                                    s.artist_id,
                                    l.sessionId as session_id,
                                    l.location,
                                    l.userAgent as user_agent
                                FROM logs l
                                LEFT JOIN songs s ON
                                    l.song = s.title AND
                                    l.artist = s.artist_name 
                                """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.parquet(output_data+'./songplays_table/', partitionBy=["year","month"], mode='overwrite')
                            
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-sy-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
