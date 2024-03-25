from googleapiclient.discovery import build
import pandas as pd
import time
import datetime

"""#**Youtube video data extraction and loading to BigQuery**

## Extract Youtube data from the API
"""

api_key= '[Insert your API Key]'
youtube=build('youtube','v3', developerKey=api_key)

# Get a list of channel_ids from youtube


#Economia:
set_1=['UCBLCvUUCiSqBCEc-TqZ9rGw',
       'UCZ4AMrDcNrfy3X6nsU8-rPg',
       'UC1tTYKft0v94eTqvS_ihJug',
       'UCkCGANrihzExmu9QiqZpPlQ']

#Data
set_2=['UC7cs8q-gJRlGwj4A8OmCmXg',
       'UCLLw7jmFsvfIVaUFsLs8mlQ',
       'UCW8Ews7tdKKkBT6GdtQaXvQ',
       'UCxladMszXan-jfgzyeIMyvw',
       'UC8butISFwT-Wl7EV0hUK0BQ']

#Inversiones
set_3=['UCCVIYA5kpLvEToE8Gj8Fszw',
       'UCvSXMi2LebwJEM1s4bz5IBA',
       'UCG8XPKMYb_nXtzmiPUoHMWg']

#Historia
set_4=['UCMmaBzfCCwZ2KqaBJjkj0fw',
       'UCBIMW0ZhwULY_x7fdaPRPiQ']

# Musica
set_5=['UCN9HPn2fq-NL8M5_kp4RWZQ',
        ]

#Entretenimiento
set_6=['UCxIBZ-nQhgZES3UaTm8eDPA',
       'UCCNgRIfWQKZyPkNvHEzPh7Q',]

#Politica
set_7=['UCJQQVLyM6wtPleV4wFBK06g',
       'UChvUiqadVeBU-_xMi5el2Aw',
       'UCTqb7oZzCYpzOhPenq6AOyQ']


channel_ids=None

# Obtener el día de la semana (0 es lunes, 6 es domingo)
def get_set():

    today = datetime.datetime.today().weekday()
    
    # Change set of channels based on the weekday()
    if today == 0:
        channel_ids = set_1
    elif today == 1:
        channel_ids = set_2
    elif today == 2:
        channel_ids = set_3
    elif today == 3:
        channel_ids = set_4
    elif today == 4:
        channel_ids = set_5
    elif today == 5:
        channel_ids = set_6
    elif today == 6:
        channel_ids = set_7
    return channel_ids

channel_ids=get_set()

"""###Function to get channel statistics"""

# Set a function to get the channel_id, channel name and statistical data:

def get_channel_data(youtube,channel_ids):
    all_data=[]
    request = youtube.channels().list(part='id,snippet,statistics',
                                      id=','.join(channel_ids))
    response=request.execute()

    for i in response['items']:
      data=dict(channel_name=i['snippet']['title'],
              id_channel=i['id'],
              view_count      =int(i['statistics']['viewCount']),
              subscriber_count=int(i['statistics']['subscriberCount']),
              video_count =int(i['statistics']['videoCount']),
              date_created=i['snippet']['publishedAt'].split('T')[0])
      all_data.append(data)
      df=pd.DataFrame(all_data)
    return df


#Execute the function declaring a variable channels_data:
channels=get_channel_data(youtube,channel_ids)


"""### Function for Video IDs"""

def get_video_ids(youtube,channel_ids):

# Make a Function to get all videoIDs from a channel:
    def get_video_ids(youtube,channel_id):

       request = youtube.search().list(part='snippet,id',
                                       channelId=channel_id,
                                       maxResults=50,
                                       order='date')
       response=request.execute()
       video_ids=[]

       time.sleep(1)

    #Get video_IDs with a For loop to append it to video_ids list:
       for video in response['items']:
            if video['id']['kind']=='youtube#video':
                video_id= video['id']['videoId']
                video_ids.append(video_id)


     #Make the pagination in order to get all the results for the query:
       next_page_token=response.get('nextPageToken')
       more_pages=True

       while more_pages:
           if next_page_token is None:
             more_pages=False
           else:
               request = youtube.search().list(part='snippet,id',
                                            channelId=channel_id,
                                            maxResults=50,
                                            order='date',
                                            pageToken=next_page_token)
               response=request.execute()

               for video in response['items']:
                 if video['id']['kind']=='youtube#video':
                     video_id= video['id']['videoId']
                     video_ids.append(video_id)

               next_page_token=response.get('nextPageToken')

       return video_ids
    video_ids_list=[]

    for channel_id in channel_ids:
      video_ids_list.extend(get_video_ids(youtube,channel_id))

    return video_ids_list

video_ids=get_video_ids(youtube,channel_ids)

"""###Function for video details"""

#Set a function to get video details from the video_ids list:
def get_video_details(youtube,video_ids):

  all_videos=[]
  for i in range(0,len(video_ids),50):
    request = youtube.videos().list(part='snippet,id,statistics',
                                         id=','.join(video_ids[i:i+50]))
    response=request.execute()

    for video in response['items']:
        videos=dict(channel_name  =video['snippet']['channelTitle'],
                    video_title   =video['snippet']['title'],
                    video_id      =video['id'],
                    date_published=video['snippet']['publishedAt'].split('T')[0],
                    viewcount     =int(video['statistics'].get('viewCount',0)),
                    likecount     =int(video['statistics'].get('likeCount',0)),
                    commentcount  =int(video['statistics'].get('commentCount',0)))
        all_videos.append(videos)

#Convert results into a dataframe:
        df=pd.DataFrame(all_videos)

  return df

videos=get_video_details(youtube,video_ids)



"""# Load to AWS Database"""

#!pip install psycopg2

import psycopg2 as ps



endpoint='localhost'
dbname= 'youtube_local'
port='5432'
user='postgres'
password='aure0006'

conn=None

def conect_to_db(endpoint,dbname,user,password,port):
    try:
        conn=ps.connect(host=endpoint,database=dbname,user=user,password=password,port=port)

    except ps.OperationalError as e:
      raise e
    else:
      print('conectado')
    return conn
    
conn=conect_to_db(endpoint,dbname,user,password,port)

#Create table for videos
def create_table_videos(curr):

     create_table_command=(
            ''' CREATE TABLE IF NOT EXISTS videos (
            channel_name    VARCHAR(255) NOT NULL ,
            video_title	    TEXT NOT NULL,
            video_id	      VARCHAR(255) PRIMARY KEY,
            date_published	DATE NOT NULL DEFAULT CURRENT_DATE,
            viewcount	      BIGINT  NOT NULL,
            likecount	      INTEGER NOT NULL,
            commentcount    INTEGER NOT NULL
            )
            ''')
     curr.execute(create_table_command)

def create_table_channels(curr):

     create_table_command=(
            ''' CREATE TABLE IF NOT EXISTS channels (
            channel_name    VARCHAR(255) NOT NULL ,
            id_channel	      VARCHAR(255) PRIMARY KEY,
            view_count	      BIGINT  NOT NULL,
            subscriber_count	    INTEGER  NOT NULL,
            video_count	      INTEGER NOT NULL,
            date_created	    DATE NOT NULL DEFAULT CURRENT_DATE
            )
            ''')
     curr.execute(create_table_command)

def check_if_video_exists(curr,video_id):

    query=('''
    SELECT video_id FROM videos WHERE video_id= %s
    ''')
    curr.execute(query,(video_id,))
    return curr.fetchone() is not None

def check_if_channel_exists(curr,id_channel):

    query=('''
    SELECT id_channel FROM channels WHERE id_channel= %s
    ''')
    curr.execute(query,(id_channel,))
    return curr.fetchone() is not None

def update_videos(curr,channel_name,video_title,video_id,date_published,viewcount,likecount,commentcount):
  query=('''UPDATE videos
  SET channel_name = %s,
  video_title	  = %s,
  date_published= %s,
  viewcount	    = %s,
  likecount	    = %s,
  commentcount  = %s
  WHERE video_id= %s;
  ''')

  vars_to_update= (	channel_name,video_title,date_published,viewcount,likecount,commentcount,video_id)
  curr.execute(query,vars_to_update)

def update_channels(curr,channel_name,	id_channel,view_count,subscriber_count,video_count,date_created):
  query=('''UPDATE channels
  SET channel_name = %s,
  view_count	  = %s,
  subscriber_count= %s,
  video_count	    = %s,
  date_created	    = %s
  WHERE id_channel= %s;
  ''')
  vars_to_update= (channel_name,view_count,subscriber_count,video_count,date_created,id_channel)
  curr.execute(query,vars_to_update)

#updating the database
#handling data for scalability

def update_db_videos(curr,df):
    tmp_df=pd.DataFrame(columns=['channel_name','video_title','video_id','date_published','viewcount','likecount','commentcount'])

    #check to see if video exists

    for i, row in df.iterrows():
      if check_if_video_exists(curr,row['video_id']): #if vedeo exists, update
        update_videos(curr,row['channel_name'],row['video_title'],row['video_id'],row['date_published'],row['viewcount'],row['likecount'],row['commentcount'])
      else: #if not, append to the DataFrame
        tmp_df = pd.concat([tmp_df,pd.DataFrame([row],columns=tmp_df.columns)])


    return tmp_df



def update_db_channels(curr,df):
    tmp_df=pd.DataFrame(columns=['channel_name','id_channel','view_count','subscriber_count','video_count','date_created'])

    #check to see if video exists

    for i, row in df.iterrows():
      if check_if_channel_exists(curr,row['id_channel']): #if vedeo exists, update
        update_channels(curr,row['channel_name'],row['id_channel'],row['view_count'],row['subscriber_count'],row['video_count'],row['date_created'])
      else: #if not, append to the DataFrame
        tmp_df = pd.concat([tmp_df,pd.DataFrame([row],columns=tmp_df.columns)])


    return tmp_df

#perform update on exixting videos

def insert_into_videos(curr,channel_name,video_title,video_id,date_published,viewcount,likecount,commentcount):

    insert_into_videos= ('''
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
    rows_to_insert=(channel_name,video_title,video_id,date_published,viewcount,likecount,commentcount)

    curr.execute(insert_into_videos,rows_to_insert)



#perform update on exixting channels
def insert_into_channels(curr,channel_name,id_channel,view_count,subscriber_count,video_count,date_created):

    insert_into_channels= ('''
     INSERT INTO channels (channel_name,id_channel,view_count,subscriber_count,video_count,date_created)
     VALUES(%s,%s,%s,%s,%s,%s)
     ON CONFLICT (id_channel) DO UPDATE
     SET channel_name = EXCLUDED.channel_name,
     view_count = EXCLUDED.view_count,
     subscriber_count = EXCLUDED.subscriber_count,
     video_count = EXCLUDED.video_count,
     date_created = EXCLUDED.date_created;
    ''')
    rows_to_insert=(channel_name,id_channel,view_count,subscriber_count,video_count,date_created)
    curr.execute(insert_into_channels,rows_to_insert)

def append_from_df_to_videos(curr,df):
    for i,row in df.iterrows():
      insert_into_videos(curr,row['channel_name'],row['video_title'],row['video_id'],row['date_published'],row['viewcount'],row['likecount'],row['commentcount'])




def append_from_df_to_channels(curr,df):
    for i,row in df.iterrows():
      insert_into_channels(curr,row['channel_name'],row['id_channel'],row['view_count'],row['subscriber_count'],row['video_count'],row['date_created'])

#Create the cursor
curr=conn.cursor()

create_table_videos(curr)
create_table_channels(curr)

new_vid_df=update_db_videos(curr,videos)
new_chan_df=update_db_channels(curr,channels)

append_from_df_to_videos(curr,new_vid_df)
append_from_df_to_channels(curr,new_chan_df)

#Commit commands to the DB
conn.commit()

#Close connection

curr.close()
conn.close()


#display(channels)
