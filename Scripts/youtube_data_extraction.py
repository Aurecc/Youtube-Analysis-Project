
from googleapiclient.discovery import build
import pandas as pd
import time
import datetime


from api_keys import youtube_api_key

youtube = build('youtube','v3', developerKey=youtube_api_key)

# Get a list of channel_ids from youtube


# Monday set:
sets = dict(set_1 = ['UCBLCvUUCiSqBCEc-TqZ9rGw',
       'UCZ4AMrDcNrfy3X6nsU8-rPg',
       'UC1tTYKft0v94eTqvS_ihJug',
       'UCkCGANrihzExmu9QiqZpPlQ'],

# Tuesday set:
set_2 = ['UC7cs8q-gJRlGwj4A8OmCmXg',
       'UCLLw7jmFsvfIVaUFsLs8mlQ',
       'UCW8Ews7tdKKkBT6GdtQaXvQ',
       'UCxladMszXan-jfgzyeIMyvw',
       'UC8butISFwT-Wl7EV0hUK0BQ'],

# Wednesday set:
set_3=['UCCVIYA5kpLvEToE8Gj8Fszw',
       'UCvSXMi2LebwJEM1s4bz5IBA',
       'UCG8XPKMYb_nXtzmiPUoHMWg'],

# Thursday set:
set_4 = ['UCMmaBzfCCwZ2KqaBJjkj0fw',
       'UCBIMW0ZhwULY_x7fdaPRPiQ'],

# Friday set:
set_5 = ['UCN9HPn2fq-NL8M5_kp4RWZQ',
        ],

# Saturday set:
set_6 = ['UCxIBZ-nQhgZES3UaTm8eDPA',
       'UCCNgRIfWQKZyPkNvHEzPh7Q',],

#Sunday set:
set_7 = ['UCJQQVLyM6wtPleV4wFBK06g',
       'UChvUiqadVeBU-_xMi5el2Aw',
       'UCTqb7oZzCYpzOhPenq6AOyQ']
)


channel_ids = None

# Get the set we're using for the day the code is running:
def get_set():

    today = datetime.datetime.today().weekday()
    
    # Change set of channels based on the weekday()
    for i in range(0,6):
    
        if today == i:
            channel_ids = sets[f'set_{i+1}']
    
    return channel_ids

channel_ids = get_set()


# Set a function to get the channel_id, channel name and statistical data:

def get_channel_data(youtube, channel_ids):
    all_data=[]
    request = youtube.channels().list(part='id,snippet,statistics',
                                      id=','.join(channel_ids))
    response=request.execute()

    for i in response['items']:
      data = dict(channel_name=i['snippet']['title'],
              id_channel = i['id'],
              view_count = int(i['statistics']['viewCount']),
              subscriber_count = int(i['statistics']['subscriberCount']),
              video_count = int(i['statistics']['videoCount']),
              date_created = i['snippet']['publishedAt'].split('T')[0])
      all_data.append(data)
      df = pd.DataFrame(all_data)
    return df


# Execute the function declaring a variable channels_data:
channels = get_channel_data(youtube,channel_ids)


"""### Function to get Video IDs"""

def get_video_ids(youtube, channel_ids):

# Make a Function to get all videoIDs from a channel:
    def get_video_ids(youtube, channel_id):

       request = youtube.search().list(part='snippet,id',
                                       channelId=channel_id,
                                       maxResults=50,
                                       order='date')
       response = request.execute()
       video_ids = []

       time.sleep(1)

    #Get video_IDs with a For loop to append it to video_ids list:
       for video in response['items']:
            if video['id']['kind'] == 'youtube#video':
                video_id = video['id']['videoId']
                video_ids.append(video_id)


     # Make the pagination in order to get all the results for the query:
       next_page_token = response.get('nextPageToken')
       more_pages = True

       while more_pages:
           if next_page_token is None:
             more_pages = False
           else:
               request = youtube.search().list(part='snippet,id',
                                            channelId=channel_id,
                                            maxResults=50,
                                            order='date',
                                            pageToken=next_page_token)
               response = request.execute()

               for video in response['items']:
                 if video['id']['kind'] == 'youtube#video':
                     video_id = video['id']['videoId']
                     video_ids.append(video_id)

               next_page_token = response.get('nextPageToken')

       return video_ids
           
    video_ids_list = []

    for channel_id in channel_ids:
      video_ids_list.extend(get_video_ids(youtube, channel_id))

    return video_ids_list

video_ids = get_video_ids(youtube, channel_ids)

"""###Function for video details"""

#Set a function to get video details from the video_ids list:
def get_video_details(youtube,video_ids):

  all_videos = []
  for i in range(0, len(video_ids), 50):
    request = youtube.videos().list(part='snippet, id, statistics',
                                         id=','.join(video_ids[i:i+50]))
    response = request.execute()

    for video in response['items']:
        videos = dict(channel_name  =video['snippet']['channelTitle'],
                    video_title = video['snippet']['title'],
                    video_id = video['id'],
                    date_published = video['snippet']['publishedAt'].split('T')[0],
                    viewcount     =int(video['statistics'].get('viewCount',0)),
                    likecount     =int(video['statistics'].get('likeCount',0)),
                    commentcount  =int(video['statistics'].get('commentCount',0)))
        all_videos.append(videos)

#Convert results into a dataframe:
        df = pd.DataFrame(all_videos)

  return df

videos = get_video_details(youtube, video_ids)

