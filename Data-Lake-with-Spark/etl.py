import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    """
        create a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        process data from song_data in s3 and write songs and artists tables into parquet files on s3.
        
        Parameters:
        -----------
        spark: the spark session to process data.
        input_data: path to the song_data s3 bucket.
        output_data: path to the parquet files will be stored.
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id',
                            'title',
                            'artist_id',
                            'year',
                            'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(path = output_data + 'songs/',
                              partitionBy = ['year','artist_id'],
                              mode = 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              'artist_name',
                              'artist_location',
                              'artist_latitude',
                              'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(path = output_data + 'artists/',
                                mode = 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
        process data from log_data in s3 and write users, time, and songplays tables into parquet files on s3.
        
        Parameters:
        -----------
        spark: the spark session to process data.
        input_data: path to the song_data s3 bucket.
        output_data: path to the parquet files will be stored.
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('UserId',
                              'firstName',
                              'lastName',
                              'gender',
                              'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(path = output_data + 'users/',
                              mode = 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select('datetime') \
                   .withColumn('start_time', df.datetime) \
                   .withColumn('hour', hour('datetime')) \
                   .withColumn('day', dayofmonth('datetime')) \
                   .withColumn('week', weekofyear('datetime')) \
                   .withColumn('month', month('datetime')) \
                   .withColumn('year', year('datetime')) \
                   .withColumn('day', dayofweek('datetime')) \
                   .drop('datetime') \
                   .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(path = output_data + 'time/',
                             partitionBy = ['year', 'month'],
                             mode = 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    df.createOrReplaceTempView("logs")
    song_df.createOrReplaceTempView("songs")
    songplays_table = spark.sql("""
    SELECT DISTINCT
        monotonically_increasing_id() as songplay_id,
        logs.datetime as start_time,
        logs.userId,
        logs.level,
        songs.song_id,
        songs.artist_id,
        logs.sessionId,
        logs.location,
        logs.userAgent,
        month(logs.datetime) as month,
        year(logs.datetime) as year
    FROM logs
    JOIN songs 
    ON logs.song = songs.title AND logs.artist = songs.artist_name
    WHERE logs.page = 'NextSong'
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(path = output_data + 'songplays/',
                                  partitionBy = ['year','month'],
                                  mode = 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aws-logs-103767966526-us-west-2/datalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
