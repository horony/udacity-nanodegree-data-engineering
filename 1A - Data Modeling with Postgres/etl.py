import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """
    Read and parse song-data from .json. 
    Insert parsed data into the two dimension tables (songs and artists) in the sparkify postgres database.
    """
       
    # open song file
    df = pd.read_json(filepath, lines = True)
    #print(df.head())

    # insert artist record
    artist_data = df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)
    
    # insert song record
    song_data = df[["song_id","title","artist_id","year","duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    

def process_log_file(cur, filepath):
    
    """
    Read and parse Sparkify log-data from .json. 
    Insert parsed data into two dimension tables (time and users) in the sparkify postgres database.  
    Use parsed data in combination with existing dimension tables to insert data into a fact table in the sparkify postgres database (songplays).
    """
    
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_df = df[['ts']].copy()
    
    time_df.loc[:,"hour"] = time_df['ts'].dt.hour
    time_df.loc[:,"day"] = time_df['ts'].dt.day
    time_df.loc[:,"week"] = time_df['ts'].dt.week
    time_df.loc[:,"month"] = time_df['ts'].dt.month
    time_df.loc[:,"year"] = time_df['ts'].dt.year
    time_df.loc[:,"weekday"] = time_df['ts'].dt.weekday
    time_df = time_df.drop_duplicates()
    
    for i, row in time_df.iterrows():
        #print(row)
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']].copy()
    user_df = user_df.drop_duplicates()
    
    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [i, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    
    """
    Listing all relavant .json-Files in given directory.
    Applying song-processing-funcition or log-processing-funcition onto the .json.
    """    
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    """
    Establish connection to sparkify postgres database.
    Enter path to json log- and song-data as well as call to function which processes the .json data.
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()