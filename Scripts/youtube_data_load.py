
# Load Data to Database

import psycopg2 as ps
from youtube_data_extraction import channels, videos

# Introduce all database required data:

from databases import youtube_aws    

channels()
videos()

endpoint = youtube_aws['endpoint']
dbname = youtube_aws['dbname']
port = youtube_aws['dbname']
user = youtube_aws['user']
password = youtube_aws['password']

conn = None

# Stablish connection whith database:
def conect_to_db(endpoint, dbname, user, password, port):
    try:
        conn = ps.connect(host=endpoint, database=dbname, user=user, password=password, port=port)

    except ps.OperationalError as e:
      raise e
    else:
      print('Connection stablished succesfully.')
    return conn
    
conn = conect_to_db(endpoint, dbname, user, password, port)


#Create table for videos
def create_table_videos(curr):

     create_table_command = (
            ''' CREATE TABLE IF NOT EXISTS videos (
            channel_name VARCHAR(255) NOT NULL ,
            video_title	TEXT NOT NULL,
            video_id VARCHAR(255) PRIMARY KEY,
            date_published	DATE NOT NULL DEFAULT CURRENT_DATE,
            viewcount BIGINT  NOT NULL,
            likecount INTEGER NOT NULL,
            commentcount INTEGER NOT NULL
            )
            ''')
     curr.execute(create_table_command)

# Create table for the channels:
def create_table_channels(curr):

     create_table_command = (
            ''' CREATE TABLE IF NOT EXISTS channels (
            channel_name VARCHAR(255) NOT NULL ,
            id_channel VARCHAR(255) PRIMARY KEY,
            view_count BIGINT  NOT NULL,
            subscriber_count INTEGER  NOT NULL,
            video_count	 INTEGER NOT NULL,
            date_created DATE NOT NULL DEFAULT CURRENT_DATE
            )
            ''')
     curr.execute(create_table_command)

# Functions to check if a video or channel in the set already exists:
def check_if_video_exists(curr,video_id):

    query = ('''
    SELECT video_id FROM videos WHERE video_id= %s
    ''')
    curr.execute(query, (video_id,))
    return curr.fetchone() is not None

def check_if_channel_exists(curr,id_channel):

    query = ('''
    SELECT id_channel FROM channels WHERE id_channel= %s
    ''')
    curr.execute(query, (id_channel,))
    return curr.fetchone() is not None

# Functions to update the channels and videos table:

def update_videos(curr, channel_name, video_title, video_id, date_published, viewcount, likecount, commentcount):
  query = ('''UPDATE videos
  SET channel_name = %s,
  video_title = %s,
  date_published = %s,
  viewcount = %s,
  likecount = %s,
  commentcount = %s
  WHERE video_id = %s;
  ''')

  vars_to_update = (channel_name, video_title, date_published, viewcount, likecount, commentcount, video_id)
  curr.execute(query, vars_to_update)

def update_channels(curr, channel_name, id_channel, view_count, subscriber_count, video_count, date_created):
  query=('''UPDATE channels
  SET channel_name = %s,
  view_count = %s,
  subscriber_count = %s,
  video_count = %s,
  date_created = %s
  WHERE id_channel = %s;
  ''')
  vars_to_update = (channel_name, view_count, subscriber_count, video_count, date_created, id_channel)
  curr.execute(query, vars_to_update)

# Updating the database and handling data for scalability

def update_db_videos(curr, df):
    tmp_df = pd.DataFrame(columns = ['channel_name','video_title',
                                   'video_id','date_published',
                                   'viewcount','likecount','commentcount'])

    #check to see if video exists

    for i, row in df.iterrows():
      if check_if_video_exists(curr, row['video_id']): # if video exists, update
        update_videos(curr, row['channel_name'], row['video_title'],
                      row['video_id'], row['date_published'],
                      row['viewcount'], row['likecount'], row['commentcount'])
             
      else: # if not, append to the DataFrame
        tmp_df = pd.concat([tmp_df,pd.DataFrame([row], columns=tmp_df.columns)])

    return tmp_df



def update_db_channels(curr, df):
    tmp_df = pd.DataFrame(columns = ['channel_name', 'id_channel', 'view_count',
                                 'subscriber_count', 'video_count', 'date_created'])

    for i, row in df.iterrows():
      if check_if_channel_exists(curr, row['id_channel']): #if channel exists, update
        update_channels(curr, row['channel_name'], row['id_channel'], row['view_count'],
            row['subscriber_count'], row['video_count'], row['date_created'])
      else: #if not, append to the DataFrame
        tmp_df = pd.concat([tmp_df, pd.DataFrame([row], columns=tmp_df.columns)])


    return tmp_df

# Perform update on exixting videos

def insert_into_videos(curr, channel_name, video_title, video_id, date_published, viewcount, likecount, commentcount):

    insert_into_videos = ('''
     INSERT INTO videos (channel_name, video_title, video_id, date_published, viewcount, likecount, commentcount)
     VALUES(%s,%s,%s,%s,%s,%s,%s)
     ON CONFLICT (video_id) DO UPDATE
     SET channel_name = EXCLUDED.channel_name,
     video_title = EXCLUDED.video_title,
     date_published = EXCLUDED.date_published,
     viewcount = EXCLUDED.viewcount,
     likecount = EXCLUDED.likecount,
     commentcount = EXCLUDED.commentcount;
    ''')
    rows_to_insert = (channel_name, video_title, video_id, date_published, viewcount, likecount, commentcount)

    curr.execute(insert_into_videos, rows_to_insert)


# Perform update on exixting channels
def insert_into_channels(curr, channel_name, id_channel, view_count, subscriber_count, video_count, date_created):

    insert_into_channels = ('''
     INSERT INTO channels (channel_name, id_channel, view_count, subscriber_count, video_count, date_created)
     VALUES(%s, %s, %s, %s, %s, %s)
     ON CONFLICT (id_channel) DO UPDATE
     SET channel_name = EXCLUDED.channel_name,
     view_count = EXCLUDED.view_count,
     subscriber_count = EXCLUDED.subscriber_count,
     video_count = EXCLUDED.video_count,
     date_created = EXCLUDED.date_created;
    ''')
    rows_to_insert = (channel_name, id_channel, view_count, subscriber_count, video_count, date_created)
    curr.execute(insert_into_channels, rows_to_insert)

def append_from_df_to_videos(curr, df):
    for i,row in df.iterrows():
      insert_into_videos(curr, row['channel_name'], row['video_title'],
                         row['video_id'], row['date_published'],
                         row['viewcount'], row['likecount'], row['commentcount'])




def append_from_df_to_channels(curr, df):
    for i,row in df.iterrows():
      insert_into_channels(curr, row['channel_name'], row['id_channel'],
                           row['view_count'], row['subscriber_count'],
                           row['video_count'], row['date_created'])

# Create the cursor
curr = conn.cursor()

create_table_videos(curr)
create_table_channels(curr)

new_vid_df = update_db_videos(curr, videos)
new_chan_df = update_db_channels(curr, channels)

append_from_df_to_videos(curr, new_vid_df)
append_from_df_to_channels(curr, new_chan_df)

# Commit commands to the DB
conn.commit()

#Close connection
curr.close()
conn.close()