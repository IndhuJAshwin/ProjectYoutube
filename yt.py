#Dash board library
import streamlit as st

# Comfiguring Streamlit GUI
st.set_page_config(layout='wide')
#Title
st.title(':black[Youtube Data Harvesting]')

# Data collection zone
st.header(':violet[Data collection zone]')
st.write ('(Note:- This zone **collect data** by using channel id and **stored it in the :green[MongoDB] database**.)')
channel_id = st.text_input('**Enter 11 digit channel_id**')
st.write('''Get data and stored it in the MongoDB database to click below **:blue['Get data and stored']**.''')
key = st.button('**Get data and stored**')

# Youtube API library
import googleapiclient.discovery
from googleapiclient.discovery import build
# Create a YouTube API client
youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey='AIzaSyA8QZDyoVymyfkY3Vy8JYTsezusviaG7uA')

# Defining function to get channel data
def get_channel_data(youtube,channel_id):
# Fetch channel details
    channel_response = youtube.channels().list(
    id=channel_id,
    part='snippet,statistics,contentDetails')
    channel_detls = channel_response.execute()
# Extract relevant information
    c_data = dict ( Channel_name = channel_detls['items'][0]['snippet']['title'],
                    Channel_ID = channel_detls['items'][0]['id'],
                    Subscribers = channel_detls['items'][0]['statistics']['subscriberCount'],
                    Views = channel_detls['items'][0]['statistics']['viewCount'],
                    Description = channel_detls['items'][0]['snippet']['description'],
                    Total_Videos = channel_detls['items'][0]['statistics']['videoCount'],
                    Playlist_id = channel_detls['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                  )
    return c_data

# Defining function to get Video_ID Details from Playlist:
def get_videoID_data(youtube,playlist_id):
# Fetch playlist ID
    vid_details = []
    request = youtube.playlistItems().list(
    part='contentDetails',
    playlistId= playlist_id,
    maxResults=50)
    response = request.execute()
    for item in response['items']:
      vid_details.append(item['contentDetails']['videoId'])
    return vid_details

 # Defining function to get video details of each video in the playlist:
def get_videodetails(youtube,video_IDs):
# Fetch video details
  data=[]
  for i in video_IDs:
    request = youtube.videos().list(
    part='snippet,statistics,contentDetails',
    id= i )
    response = request.execute()
# Extract relevant information
    data.append ({"Video_ID": i,
                "Channel_Id": response['items'][0]['snippet']['channelId'], 
                "Video_Name": response['items'][0]['snippet']['title'],
                "Video_Description":response['items'][0]['snippet']['description'],
                "Video_Statistics": response['items'][0]['statistics']['commentCount'],
                "Comment_Count": response['items'][0]['statistics'].get('commentCount', 0),
                "View_Count": response['items'][0]['statistics'].get('viewCount', 0),
                "Like_Count": response['items'][0]['statistics'].get('likeCount', 0),
                "Favorite_Count": response['items'][0]['statistics'].get('favoriteCount', 0),
                "Published_At": response['items'][0]['snippet']['publishedAt'],
                "Duration": response['items'][0]['contentDetails']['duration'],
                "Thumbnail": response['items'][0]['snippet']['thumbnails']['default']['url'],
                "Caption_Status": response['items'][0]['contentDetails'].get('caption')
                })
  return data

# Defining function to get Commands of each videos
def get_comments_details(youtube,videoID_data):
# Fetch comment details
    v_c_data=[]
    for i in videoID_data:
        try:
            comments_response = youtube.commentThreads().list(
            part='snippet',
            maxResults=2,  # only 2 comments
            videoId=i)
            response=comments_response.execute()
            comments = response['items']
# Extract relevant information
            for comment in comments:
                comment_information = {
                                  "Video_iD": i,
                                  "Comment_Id": comment['snippet']['topLevelComment']['id'],
                                  "Comment_Text": comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                                  "Comment_Author": comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                  "Comment_PublishedAt": comment['snippet']['topLevelComment']['snippet']['publishedAt']
                                  }
            v_c_data.append(comment_information)
        except:
          pass
    return v_c_data

# Defining main function
def main(channel_id):
  c = get_channel_data(youtube,channel_id)
  p = get_videoID_data(youtube,c['Playlist_id'])
  v = get_videodetails(youtube,p)
  cm = get_comments_details(youtube,p)
# Assinging datas into a variable
  data = {'channel details':c,
          'playlist details':p,
          'video details':v,
          'comment details':cm}
  return data

channel_id = 'UCDnq05Q89oYq-Tz5boL73Tw'
d = main(channel_id)

# Data Migration (converting unstructured data to structured data)
# store data to SQLite
st.header(':orange[Data Migrate zone]')
st.write ('''(Note:- This zone specific channel data **Migrate to :blue[SQLite] database from  :green[MongoDB] database** depending on your selection,
                if unavailable your option first collect data.)''')

#MongoDB,Pandas,Numpy
import pymongo
from pymongo import MongoClient
import pandas as pd
import numpy as np

# MongoDB connection and store the collected data
# Connect to mongodb atlas
client = MongoClient("mongodb+srv://indhujagadeeswaran:sumathi09@cluster0.ki2kcii.mongodb.net/?retryWrites=true&w=majority")

# Create mongodb database and collection
db = client['database_youtube']
collection = db['collection_channel']

#Insert channel data
collection.insert_one(d)

#SQLite Migration
all_data= st.selectbox('**Select Channel name**', options = d, key= d)
st.write('''Migrate to SQLite database from MongoDB database to click below **:blue['Migrate to SQLite']**.''')
Migrate = st.button('**Migrate to SQLite**')

# Define Session state to Migrate to MySQL button
if 'migrate_sql' not in st.session_state:
        st.session_state_migrate_sql = False
if Migrate or st.session_state_migrate_sql:
        st.session_state_migrate_sql = True
        
# Retrieve the document with the specified name
result = d['channel details']
print(d)

#SQLite
import sqlite3

# Data Migrate to SQLite
# Connect to the SQLite database
con = sqlite3.connect("youtubeSQLite.db")
cur = con.cursor()

#Create Channel Table
cur.execute("CREATE TABLE IF NOT EXISTS Channel(Channel_name TEXT,Channel_ID TEXT,Subscribers INTEGER,Views INTEGER,Description TEXT,Total_Videos INTEGER,Playlist_id TEXT)")
cur.execute(f"""INSERT INTO Channel VALUES {tuple(result.values())}""")
con.commit()

# Retrieve the document with the specified name
result1 = d['video details'][0]

#Create Video Table
cur.execute("CREATE TABLE IF NOT EXISTS Video(Video_ID TEXT,Channel_ID TEXT,Video_Name TEXT,Video_Description TEXT,Video_Statistics INTEGER,Comment_Count INTEGER,View_Count INTEGER,Like_Count INTEGER,Favorite_Count INTEGER,Published_At INTEGER,Duration TEXT,Thumbnail TEXT,Caption_Status TEXT)")
cur.execute(f"""INSERT INTO Video VALUES {tuple(result1.values())}""")
con.commit()

# Retrieve the document with the specified name
result2 = d['comment details'][0]

#Create Comment Table
cur.execute("CREATE TABLE IF NOT EXISTS Comment(Video_Id TEXT,Comment_Id TEXT,Comment_Text TEXT,Comment_Author TEXT,Comment_PublishedAt INTEGER)")
cur.execute(f"""INSERT INTO Comment VALUES {tuple(result2.values())}""")
con.commit()

#Dash board libraries
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns

#File handling
import json 

# Questions for Analysing Data
st.subheader(':violet[Channels Analysis]')

question_tosql = st.selectbox('**Select your Question**',
        ('1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?',
        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
        '8. What are the names of all the channels that have published videos in the year 2022?',
        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10. Which videos have the highest number of comments, and what are their corresponding channel names?'), key = 'collection_question')

con_for_question = sqlite3.connect("youtubeSQLite.db")
cur = con_for_question.cursor()

# Q1
if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
    cur.execute("select channel.Channel_name,video.Video_Name from video inner join channel on video.Channel_ID=channel.Channel_ID;")
    result_1 = cur.fetchall()
    df1 = pd.DataFrame(result_1, columns=['Channel Name', 'Video Name']).reset_index(drop=True)
    df1.index += 1
    st.dataframe(df1)
    
# Q2
elif question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':

    col1,col2 = st.columns(2)
    with col1:
        cur.execute("SELECT Channel_name, Total_Videos FROM channel ORDER BY Total_Videos DESC;")
        result_2 = cur.fetchall()
        df2 = pd.DataFrame(result_2,columns=['Channel Name','Total Videos']).reset_index(drop=True)
        df2.index += 1
        st.dataframe(df2)

    with col2:
        fig_vc = px.bar(df2, y='Total Videos', x='Channel Name', text_auto='.2s', title="Most number of videos", )
        fig_vc.update_traces(textfont_size=16,marker_color='#E6064A')
        fig_vc.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
        st.plotly_chart(fig_vc,use_container_width=True)   

# Q3
elif question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':

    col1,col2 = st.columns(2)
    with col1:
        cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.View_Count FROM channel JOIN video ON channel.Channel_Id = video.Channel_Id  ORDER BY video.View_Count DESC LIMIT 10;")
        result_3 = cursor.fetchall()
        df3 = pd.DataFrame(result_3,columns=['Channel Name', 'Video Name', 'View count']).reset_index(drop=True)
        df3.index += 1
        st.dataframe(df3)

    with col2:
        fig_topvc = px.bar(df3, y='View count', x='Channel Name', text_auto='.2s', title="Top 10 most viewed videos")
        fig_topvc.update_traces(textfont_size=16,marker_color='#E6064A')
        fig_topvc.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
        st.plotly_chart(fig_topvc,use_container_width=True)
        
# Q4
elif question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
    cur.execute("SELECT video.Video_Name, video.Comment_Count FROM video JOIN comment ON video.Video_Id = comment.Video_Id;")
    result_4 = cur.fetchall()
    df4 = pd.DataFrame(result_4,columns=['Video Name', 'Comment count']).reset_index(drop=True)
    df4.index += 1
    st.dataframe(df4)
    
# Q5
elif question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    cur.execute("SELECT channel.Channel_Name, video.Video_Name, video.Like_Count FROM channel JOIN video ON channel.Channel_Id = video.Channel_Id ORDER BY video.Like_Count DESC;")
    result_5= cur.fetchall()
    df5 = pd.DataFrame(result_5,columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
    df5.index += 1
    st.dataframe(df5)
    
# Q6
elif question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    st.write('**Note:- In November 2021, YouTube removed the public dislike count from all of its videos.**')
    cur.execute("SELECT channel.Channel_Name, video.Video_Name, video.Like_Count  FROM channel JOIN video ON channel.Channel_ID = video.Channel_ID ORDER BY video.Like_Count DESC;")
    result_6= cur.fetchall()
    df6 = pd.DataFrame(result_6,columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
    df6.index += 1
    st.dataframe(df6)
    
# Q7
elif question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':

    col1, col2 = st.columns(2)
    with col1:
        cur.execute("SELECT channel.Channel_name, channel.Views FROM channel ORDER BY Views DESC;")
        result_7= cur.fetchall()
        df7 = pd.DataFrame(result_7,columns=['Channel Name', 'Total number of views']).reset_index(drop=True)
        df7.index += 1
        st.dataframe(df7)

    with col2:
        fig_topview = px.bar(df7, y='Total number of views', x='Channel Name', text_auto='.2s', title="Total number of views", )
        fig_topview.update_traces(textfont_size=16,marker_color='#E6064A')
        fig_topview.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
        st.plotly_chart(fig_topview,use_container_width=True)
        
# Q8
elif question_tosql == '8. What are the names of all the channels that have published videos in the year 2023?':
    cur.execute("SELECT channel.Channel_name,video.Video_Name,video.Published_At FROM channel JOIN video ON channel.Channel_ID= video.Channel_ID WHERE strftime('%Y',Published_At) ='2023';")
    result_8= cur.fetchall()
    df8 = pd.DataFrame(result_8,columns=['Channel Name','Video Name', 'Year 2023 only']).reset_index(drop=True)
    df8.index += 1
    st.dataframe(df8)
    
# Q9
elif question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    cur.execute("select channel.Channel_name,video.Duration from channel JOIN Video ON channel.Channel_ID=video.Channel_ID GROUP BY Channel_name ORDER BY Duration DESC;")
    result_9= cur.fetchall()
    df9 = pd.DataFrame(result_9,columns=['Channel Name','Duration']).reset_index(drop=True)
    df9.index += 1
    st.dataframe(df9)
    
# Q10
elif question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    cur.execute("SELECT channel.Channel_name, video.Video_Name, video.Comment_Count FROM channel JOIN video ON channel.Channel_ID= video.Channel_ID  ORDER BY VIDEO.Comment_Count DESC;")
    result_10= cur.fetchall()
    df10 = pd.DataFrame(result_10,columns=['Channel Name','Video Name', 'Number of comments']).reset_index(drop=True)
    df10.index += 1
    st.dataframe(df10)
    

# SQLite DB connection close
con_for_question.close()