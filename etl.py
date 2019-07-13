import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, IntegerType, LongType, ShortType, StringType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    The entry point to programming Spark with the Dataset and DataFrame API.
    
    Args:
        None

    Returns:
        None
    """
    print("BEGIN: create_spark_session")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("END: create_spark_session")
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song files in JSON format about a song and the artist of that song.
    
    Args:
        spark: Spark session.
        input_data: Input S3 bucket with raw Sparkify data.
        output_data: Output S3 bucket to save fact and dimension tables.

    Returns:
        None
    """
    print("BEGIN: process_song_data")
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    #Testing
    #song_data = './data/song_data/A/A/A/TRAAAPK128E0786D96.json'
    #song_data = input_data + 'song_data/A/A/A/*.json'
    
    # read song data file
    songSchema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", ShortType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", ShortType())
    ])
    df = spark.read.json(song_data, schema=songSchema, multiLine=True)

    # create staging_songs View
    df.createOrReplaceTempView("staging_songs")
    print("    staging_songs count:", df.count())
    
    # extract columns to create songs table
    songs_table = spark.sql('''
        SELECT DISTINCT song_id
                      , title
                      , artist_id
                      , year
                      , duration
          FROM staging_songs
    '''
    )

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs')
    print("    songs_table count:", songs_table.count())

    # extract columns to create artists table
    artists_table = spark.sql('''
        SELECT DISTINCT artist_id
                      , artist_name AS name
                      , artist_location AS location
                      , artist_latitude AS latitude
                      , artist_longitude AS longitude
          FROM staging_songs
    '''
    )

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists')
    print("    artists_table count:", artists_table.count())
    print("END: process_song_data")


def process_log_data(spark, input_data, output_data):
    """
    Process log files in JSON format of app activity.
    
    Args:
        spark: Spark session.
        input_data: Input S3 bucket with raw Sparkify data.
        output_data: Output S3 bucket to save fact and dimension tables.

    Returns:
        None
    """
    print("BEGIN: process_log_data")
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    #Testing
    #log_data = './data/log_data/2018-11-30-events.json'
    #log_data = input_data + 'log_data/2018/11/*.json'

    # read log data file
    logSchema = StructType([
        StructField("artist", StringType()),
        StructField("auth", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", ShortType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", DoubleType()), #Convert to Long before writing to parquet files
        StructField("sessionId", ShortType()),
        StructField("song", StringType()),
        StructField("status", ShortType()),
        StructField("ts", LongType()),
        StructField("userAgent", StringType()),
        StructField("userId", StringType()) #Convert to Short before writing to parquet files
    ])
    df = spark.read.json(log_data, schema=logSchema)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # create staging_events View
    df.createOrReplaceTempView("staging_events")
    print("    staging_events count:", df.count())
    
    # extract columns for users table    
    users_table = spark.sql('''
        SELECT DISTINCT user_id
                      , first_name
                      , last_name
                      , gender
                      , level
          FROM (SELECT ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) AS row_num
                     , CAST(userId AS short) AS user_id
                     , firstName AS first_name
                     , lastName AS last_name
                     , gender
                     , level
                     , ts
                  FROM staging_events)
         WHERE row_num = 1
    '''
    )
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users')
    print("    users_table count:", users_table.count())

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    time_table = spark.sql('''
        SELECT DISTINCT start_time
                      , EXTRACT(hour FROM start_time) AS hour
                      , EXTRACT(day FROM start_time) AS day
                      , EXTRACT(week FROM start_time) AS week
                      , EXTRACT(month FROM start_time) AS month
                      , EXTRACT(year FROM start_time) AS year
                      , EXTRACT(dayofweek FROM start_time) AS weekday
          FROM (SELECT to_timestamp(ts/1000.0) AS start_time
                  FROM staging_events)
    '''
    )    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time')
    print("    time_table count:", time_table.count())

    # read in song data to use for songplays table
    #song_df = 

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql('''
        SELECT ROW_NUMBER() OVER(ORDER BY e.ts) AS songplay_id
             , t.start_time
             , CAST(e.userId AS short) AS user_id
             , e.level
             , s.song_id
             , s.artist_id
             , e.sessionId AS session_id
             , e.location
             , e.userAgent AS user_agent
             , EXTRACT(year FROM t.start_time) AS year
             , EXTRACT(month FROM t.start_time) AS month
          FROM staging_events e
               LEFT JOIN staging_songs s
                      ON e.song = s.title
                         AND e.artist = s.artist_name
               LEFT JOIN (SELECT ts
                               , to_timestamp(ts/1000.0) AS start_time
                            FROM staging_events
                         ) t
                      ON e.ts = t.ts
    '''
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays')
    print("    songplays_table count:", songplays_table.count())
    print("END: process_log_data")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-bucket-ny/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
