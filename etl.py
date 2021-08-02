import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Reads song data from JSON filepath
    - Inserts relevant data into songs table and artists table
    - References insert queries in sql_queries.py
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    cols_songs=['song_id', 'title', 'artist_id', 'year','duration']
    cols_artists=['artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude']
    
    artist_data=df[cols_artists].values[0].tolist()
    song_data= df[cols_songs].values[0].tolist()

    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Reads songplay log data from JSON filepath
    - Inserts relevant data into times, users and songplay tables
    - References insert queries in sql_queries.py
    """

    # open log file
    df = pd.read_json(filepath, lines=True)


    # filter by NextSong action
    df = df[df['page']=='NextSong']


    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    time_data = [t,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday]
    column_labels=['timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))


    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    cols=['userId','firstName','lastName','gender','level']
    user_df=df[cols]

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
        t = pd.to_datetime(row['ts'],unit='ms')
        songplay_data = (t, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Goes through directory and finds each file matching JSON extension from directory
    - Iterates over each filepath and processes data, inserts data into tables

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
    - Connects to the sparkifydb
    - Reads from log data and song data and adds records into Sparkify Database.
    
    """

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()