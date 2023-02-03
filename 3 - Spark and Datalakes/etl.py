import configparser
from datetime import datetime
import os
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.window import Window

config = configparser.ConfigParser()
config.read('dl.cfg')
print(config.read('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Create a spark session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data_song, output_data_song):
    """
    This function reads the song_data.json from a S3 bucket.
    Creates two dimension tables songs and artists from the json-data.
    Writes the two dimension tables to an S3 bucket.
    """
    
    # get filepath to song data file
    song_data = input_data_song 
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration")
    
    # format data types    
    songs_table = (songs_table
       .withColumn("song_id", col("song_id"))
       .withColumn("title", col("title"))
       .withColumn("artist_id", col("artist_id"))
       .withColumn("year", col("year").cast("int"))
       .withColumn("duration", col("duration").cast("float"))
       )
    
    # write songs table to parquet files partitioned by year and artist
    parquet_file_path = output_data_song + '/songs/songs.parquet'    
    songs_table.write.partitionBy(["year","artist_id"]).parquet(parquet_file_path)
    
    # extract columns to create artists table
    # select unique artists
    artists_table = df.select("artist_id","artist_name","artist_location", "artist_latitude","artist_longitude").dropDuplicates(subset = ["artist_id"]).orderBy(col("artist_id").asc())
    
    # rename columns
    artists_table = artists_table.withColumnRenamed("artist_name", "name")\
       .withColumnRenamed("artist_location", "location")\
       .withColumnRenamed("artist_latitude", "latitude")\
       .withColumnRenamed("artist_longitude", "longitude")
    
    # format data types
    artists_table = (artists_table
       .withColumn("artist_id", col("artist_id"))
       .withColumn("name", col("name"))
       .withColumn("location", col("location"))
       .withColumn("latitude", col("latitude").cast("float"))
       .withColumn("longitude", col("longitude").cast("float"))
       )
    
    # write artists table to parquet files
    parquet_file_path = output_data_song + '/artists/artists.parquet'
    artists_table.write.partitionBy("artist_id").parquet(parquet_file_path)
    
def process_log_data(spark, input_data_log, output_data_log):
    """
    This function reads the log_data.json from a S3 bucket.
    Creates two dimension tables time and customers from the json-data.
    Creates one fact table songplay by combining previously written parquet-data with the json-data.
    Writes the all three to a S3 bucket.
    """
    
    # get filepath to log data file
    log_data = input_data_log

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId","firstName","lastName", "gender","level").dropDuplicates(subset = ["userId"])
    
    # rename columns
    users_table = users_table.withColumnRenamed("userId", "user_id")\
       .withColumnRenamed("firstName", "first_name")\
       .withColumnRenamed("lastName", "last_name")\

    # write users table to parquet files
    parquet_file_path = output_data_log + '/users/users.parquet'
    users_table.write.partitionBy("user_id").parquet(parquet_file_path)

    # create timestamp column from original timestamp column 
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0). strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.select("start_time").dropDuplicates()
    
    # rename column
    time_table = time_table.withColumnRenamed("timestamp", "start_time") 
    
    # generate additional columns for time table
    time_table = (time_table
       .withColumn("start_time", col("start_time").cast("timestamp"))
       .withColumn("hour", hour("start_time").cast("integer"))
       .withColumn("day", dayofmonth("start_time").cast("integer"))
       .withColumn("week", weekofyear("start_time").cast("integer"))
       .withColumn("month", month("start_time").cast("integer"))
       .withColumn("year", year("start_time").cast("integer"))
       .withColumn("weekday", date_format('start_time', 'E'))
       )

    # write time table to parquet files partitioned by year and month
    parquet_file_path = output_data_log + '/time/time.parquet'    
    time_table.write.partitionBy(["year","month"]).parquet(parquet_file_path)    

    # read in song and artist data to use for songplays table
    song_df = spark.read.parquet(output_data_log + '/songs/*.parquet')
    artist_df = spark.read.parquet(output_data_log + '/artists/*.parquet')

    # join song and artist together to obtain artist name 
    song_with_artist_df = song_df.join(artist_df, song_df.artist_id == artist_df.artist_id, "left")
    song_with_artist_df = song_with_artist_df.select(song_df["*"], artist_df["name"])
                        
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_with_artist_df, (df.song == song_with_artist_df.title) & (df.artist == song_with_artist_df.name) & (df.length == song_with_artist_df.duration), "left")

    # create a unique songplay_id with the row_number-function
    songplays_table = songplays_table.withColumn('songplay_id', F.row_number().over(Window.orderBy("start_time")))

    # Create additional year and month columns for partioning
    songplays_table = songplays_table.withColumn("start_time_year", year("start_time").cast("integer"))
    songplays_table = songplays_table.withColumn("start_time_month", month("start_time").cast("integer"))
    
    # Renaming and changing data types
    songplays_table = songplays_table.select(
        col('songplay_id'), 
        col('start_time').cast("timestamp"), 
        col('start_time_year'),
        col('start_time_month'),
        col('userId').cast("integer").alias("user_id"),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').cast("integer").alias("session_id"),
        col('location'),
        col('userAgent').alias("user_agent")
    )
    
    # write songplays table to parquet files partitioned by year and month
    parquet_file_path = output_data_log + '/songplay/songplay.parquet'
    songplays_table.write.partitionBy(["start_time_year","start_time_month"]).parquet(parquet_file_path)
    
def main():
    """
    Main function for the orchestration of this ETL
    """
    spark = create_spark_session()
    
    input_data_song = 's3://udacity-dend/song_data/*/*/*/*.json'    
    output_data_song = 's3a://lennart-udacity-datalake-bucket'
   
    process_song_data(spark, input_data_song, output_data_song)   
    
    input_data_log = 's3://udacity-dend/log_data/*/*/*.json'    
    output_data_log = 's3a://lennart-udacity-datalake-bucket'
    
    process_log_data(spark, input_data_log, output_data_log)


if __name__ == "__main__":
    main()
