import streamlit as st
from googleapiclient.discovery import build
from pymongo import MongoClient
from googleapiclient.errors import HttpError
from datetime import datetime
import re
import pandas as pd
from sqlalchemy import create_engine

@st.cache_data
def service(api_key):
    youtube = build('youtube', 'v3', developerKey=api_key)
    return youtube

def service(api_key):
    youtube = build('youtube', 'v3', developerKey=api_key)
    return youtube

def get_channel_data(youtube, channel_id):
    channel_data = {}
    try:
        request = youtube.channels().list(
            part='snippet, statistics, contentDetails, brandingSettings',
            id=channel_id
        )
        response = request.execute()
        channel_name = response['items'][0]['snippet']['title']
        subscription_count = int(response['items'][0]['statistics']['subscriberCount'])
        channel_views = int(response['items'][0]['statistics']['viewCount'])
        video_count = int(response['items'][0]['statistics']['videoCount'])
        channel_description = response['items'][0]['brandingSettings']['channel'].get('description',
                                                                                       'No description available')
        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        channel_data.update({
            'channel_name': channel_name,
            'channel_id': channel_id,
            'subscription_count': subscription_count,
            'channel_views': channel_views,
            'video_count': video_count,
            'channel_description': channel_description,
            'playlist_id': playlist_id
        })

        return channel_data
    except HttpError as e:
        print(f"An error occurred: {e}")
        return None

def vc_data(youtube, playlist_id):
    try:
        video_data = {}
        next_page_token = None

        while True:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            video_ids = []
            for item in response['items']:
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            request1 = youtube.videos().list(
                part="snippet, statistics, contentDetails",
                id=','.join(video_ids),
                maxResults=50
            )
            response1 = request1.execute()

            for item in response1['items']:
                video_id = item['id']
                video_name = item['snippet']['title']
                video_description = item['snippet']['description']
                tags = item['snippet'].get('tags', [])
                published_at = item['snippet']['publishedAt']
                view_count = item['statistics'].get('viewCount', 0)
                like_count = item['statistics'].get('likeCount', 0)
                favorite_count = item['statistics'].get('favoriteCount', 0)
                comment_count = item['statistics'].get('commentCount', 0)
                duration = item['contentDetails']['duration']
                thumbnail = item['snippet']['thumbnails']['default']['url']
                caption_status = item['contentDetails'].get('caption', False)

                # Convert published_at string to datetime object
                published_at = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")

                # Convert duration format to "hr:min:sec"
                pattern = re.compile(r'PT(\d+H)?(\d+M)?(\d+S)?')
                match = pattern.match(duration)
                if match:
                    hours = int(match.group(1)[:-1]) if match.group(1) else 0
                    minutes = int(match.group(2)[:-1]) if match.group(2) else 0
                    seconds = int(match.group(3)[:-1]) if match.group(3) else 0
                    duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                else:
                    duration = "00:00:00"
                
                
                try:
                    request2 = youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=50
                    )
                    response2 = request2.execute()

                    comments = {}
                    for comment_item in response2['items']:
                        comment_id = comment_item['id']
                        comment_text = comment_item['snippet']['topLevelComment']['snippet']['textDisplay']
                        comment_author = comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                        comment_published_at = comment_item['snippet']['topLevelComment']['snippet']['publishedAt']

                        # Convert comment_published_at string to datetime object
                        comment_published_at = datetime.strptime(comment_published_at, "%Y-%m-%dT%H:%M:%SZ")

                        comment = {
                            'comment_id': comment_id,
                            'comment_text': comment_text,
                            'comment_author': comment_author,
                            'comment_published_at': comment_published_at
                        }
                        comments[comment_id] = comment

                except HttpError as e:
                    if 'commentsDisabled' in str(e):
                        comments = {}  # Set empty comments if comments are disabled
                    else:
                        raise  # Re-raise the exception if it's not a "commentsDisabled" error

                video_data[video_id] = {
                    'video_id': video_id,
                    'video_name': video_name,
                    'video_description': video_description,
                    'tags': tags,
                    'published_at': published_at,
                    'view_count': view_count,
                    'like_count': like_count,
                    'favorite_count': favorite_count,
                    'comment_count': comment_count,
                    'duration': duration,
                    'thumbnail': thumbnail,
                    'caption_status': caption_status,
                    'comments': comments
                }

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

        return video_data

    except HttpError as e:
        print(f"An error occurred: {e}")
        return None

def store_youtube_data(channel_id, channel_data, video_data):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['youtube_data_lake']

    existing_channel = db.channels.find_one({'channel_id': channel_id})
    if existing_channel:
        st.write("Channel data already exists in MongoDB.")
        return

    channel_data['channel_id'] = channel_id
    channel_data['videos'] = video_data

    db.channels.insert_one(channel_data)
    print("Data stored in MongoDB successfully!")

def transform_mongodb_to_mysql(channel_name):
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['youtube_data_lake']
    collection = db['channels']

    # Specify the query to retrieve the document by channel name
    query = {'channel_name': channel_name}

    # Retrieve the specific document by specifying the query
    document = collection.find_one(query)

    if document:
        # Extract the channel details
        channel_data = {
            'channel_id': [document['channel_id']],
            'channel_name': [document['channel_name']],
            'subscription_count': [document['subscription_count']],
            'channel_views': [document['channel_views']],
            'video_count': [document['video_count']],
            'channel_description': [document['channel_description']],
            'playlist_id': [document['playlist_id']],
        }

        # Extract the video details and comment details
        videos = document['videos']
        video_data = []
        comment_data = []
        for video_id, video_info in videos.items():
            video_data.append({
                'video_id': video_info['video_id'],
                'playlist_id': document['playlist_id'],
                'video_name': video_info['video_name'],
                'video_description': video_info['video_description'],
                'published_date': video_info['published_at'],
                'view_count': video_info['view_count'],
                'like_count': video_info['like_count'],
                'favorite_count': video_info['favorite_count'],
                'comment_count': video_info['comment_count'],
                'duration': video_info['duration'],
                'thumbnail': video_info['thumbnail'],
                'caption_status': video_info['caption_status'],
            })
            
            for comment_id, comment_info in video_info['comments'].items():
                comment_data.append({
                    'comment_id': comment_info['comment_id'],
                    'video_id': video_info['video_id'],
                    'comment_text': comment_info['comment_text'],
                    'comment_author': comment_info['comment_author'],
                    'comment_published_at': comment_info['comment_published_at']
                })

        # Create DataFrame for channel data
        channel_df = pd.DataFrame(channel_data)

        # Create DataFrame for video data
        video_df = pd.DataFrame(video_data)
        
        # Create DataFrame for comment data
        comment_df = pd.DataFrame(comment_data)

        # Define the MySQL connection details
        host = 'localhost'
        port = 3306
        user = 'root'
        password = 'Ajith568.'
        database = 'youtube_data'
        channel_table = 'channel'
        playlist_table = 'playlist'
        video_table = 'video'
        comment_table = 'comment'

        # Create a connection to MySQL
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
        connection = engine.connect()

        # Read existing channel data from MySQL table
        existing_channel_query = f"SELECT channel_id FROM {database}.{channel_table}"
        existing_channel_data = pd.read_sql_query(existing_channel_query, engine)

        # Check for channel duplicates
        channel_duplicates = pd.merge(existing_channel_data, channel_df, on='channel_id', how='inner')

        if not channel_duplicates.empty:
            print(f"Document with channel ID {channel_df['channel_id'][0]} already exists in MySQL. Skipping channel insertion.")
        else:
            # Write the channel DataFrame to MySQL
            channel_df.to_sql(channel_table, engine, if_exists='append', index=False)
            print("Channel document inserted into MySQL.")

        # Read existing playlist data from MySQL table
        existing_playlist_query = f"SELECT playlist_id FROM {database}.{playlist_table}"
        existing_playlist_data = pd.read_sql_query(existing_playlist_query, engine)

        # Check for playlist duplicates
        playlist_duplicates = pd.merge(existing_playlist_data, channel_df, on='playlist_id', how='inner')

        if not playlist_duplicates.empty:
            print(f"Document with playlist ID {channel_df['playlist_id'][0]} already exists in MySQL. Skipping playlist insertion.")
        else:
            # Create a new DataFrame for playlist data
            playlist_data = {
                'playlist_id': [document['playlist_id']],
                'channel_id': [document['channel_id']]
            }

            playlist_df = pd.DataFrame(playlist_data)

            # Write the playlist DataFrame to MySQL
            playlist_df.to_sql(playlist_table, engine, if_exists='append', index=False)

        # Read existing video data from MySQL table
        existing_video_query = f"SELECT video_id FROM {database}.{video_table}"
        existing_video_data = pd.read_sql_query(existing_video_query, engine)

        # Check for video duplicates
        video_duplicates = pd.merge(existing_video_data, video_df, on='video_id', how='inner')

        if not video_duplicates.empty:
            print("Some videos already exist in the MySQL table. Skipping video insertion.")
        else:
            # Write the video DataFrame to MySQL
            video_df.to_sql(video_table, engine, if_exists='append', index=False)
            print("Video details inserted into MySQL.")
        
        # Read existing comment data from MySQL table
        existing_comment_query = f"SELECT comment_id FROM {database}.{comment_table}"
        existing_comment_data = pd.read_sql_query(existing_comment_query, engine)

        # Check for comment duplicates
        comment_duplicates = pd.merge(existing_comment_data, comment_df, on='comment_id', how='inner')

        if not comment_duplicates.empty:
            print("Some comments already exist in the MySQL table. Skipping comment insertion.")
        else:
            # Write the comment DataFrame to MySQL
            comment_df.to_sql(comment_table, engine, if_exists='append', index=False)
            print("Comment details inserted into MySQL.")

        # Close the MySQL connection
        connection.close()

        # Close the MongoDB connection
        client.close()
        return channel_df

    else:
        print(f"No document found for channel name: {channel_name}")
        return None

def main():
    st.title("YouTube Data Extraction and Storage")

    api_key = 'AIzaSyCRK_VITtlgN4odgCAwW5g2sdIChbXVbVY'
    channel_id = st.text_input("Enter YouTube Channel ID:")
    if api_key and channel_id:
        if st.button("Extract and Store Data"):
            youtube = service(api_key)
            channel_data = get_channel_data(youtube, channel_id)
            if channel_data:
                playlist_id = channel_data['playlist_id']
                video_data = vc_data(youtube, playlist_id)
                if video_data:
                    store_youtube_data(channel_id, channel_data, video_data)
                    st.write("YouTube data stored in MongoDB.")
    # Get channel names from MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['youtube_data_lake']
    collection = db['channels']
    channel_names = collection.distinct('channel_name')

    # Display selection box for channel names
    channel_name = st.selectbox("Select Channel Name:", channel_names)

    if st.button("Transform MongoDB Data to MySQL"):
        transform_mongodb_to_mysql(channel_name)
        st.write("Data transformed from MongoDB to MySQL.")

if __name__ == '__main__':
    main()
    
st.write("Select question :")
engine = create_engine('mysql+pymysql://root:Ajith568.@localhost:3306/youtube_data')
selection = st.selectbox('', ['1. What are the names of all the videos and their corresponding channels?',
                '2. Which channels have the most number of videos and how many videos do they have?',
                '3. What are the top 10 most viewed videos and their respective channels?',
                '4. How many comments were made on each video and what are their corresponding video names?',
                '5. Which videos have the highest number of likes and what are their corresponding channel names?',
                '6. What is the total number of likes for each video and what are their corresponding video names?',
                '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                '8. What are the names of all the channels that have published videos in the year 2022?',
                '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])


if selection == '1. What are the names of all the videos and their corresponding channels?':  
    query = '''SELECT channel.channel_name, video.video_name 
            FROM channel JOIN playlist 
            JOIN video ON channel.channel_id = playlist.channel_id AND playlist.playlist_id = video.playlist_id'''
    result = pd.read_sql_query(query, engine)
    engine.dispose()
    st.dataframe(result)
elif selection == '2. Which channels have the most number of videos and how many videos do they have?':
    query = '''SELECT channel_Name, video_count 
            FROM channel 
            ORDER BY video_count DESC'''
    result = pd.read_sql_query(query, engine)
    engine.dispose()
    st.table(result)
elif selection == '3. What are the top 10 most viewed videos and their respective channels?':
    query = '''SELECT channel.channel_name, video.video_name, video.view_count 
            FROM channel 
            JOIN playlist ON channel.channel_id = playlist.channel_id 
            JOIN video ON playlist.playlist_id = video.playlist_id 
            ORDER BY video.view_count DESC LIMIT 10'''
    result = pd.read_sql_query(query, engine)
    engine.dispose()
    st.table(result)
elif selection == '4. How many comments were made on each video and what are their corresponding video names?':
    query = '''SELECT video.video_name, video.comment_count AS comment_count
            FROM video'''
    result = pd.read_sql_query(query, engine)
    engine.dispose()
    st.dataframe(result)
elif selection == '5. Which videos have the highest number of likes and what are their corresponding channel names?':
    query = '''SELECT channel.channel_name, video.video_name, video.like_count 
            FROM channel 
            JOIN playlist ON 
            channel.channel_id = playlist.channel_id JOIN video ON playlist.playlist_id = video.playlist_id 
            ORDER BY video.like_count DESC'''
    result = pd.read_sql_query(query, engine)
    engine.dispose()
    st.dataframe(result)
elif selection == '6. What is the total number of likes for each video and what are their corresponding video names?':
    query = '''SELECT video.video_name, SUM(video.like_count) AS total_likes
            FROM video
               GROUP BY video.video_name'''
    result = pd.read_sql_query(query, engine)
    engine.dispose()
    st.dataframe(result)
elif selection == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    query = '''SELECT channel.channel_name, SUM(video.view_count) AS total_views
            FROM channel
            JOIN playlist ON channel.channel_id = playlist.channel_id
            JOIN video ON playlist.playlist_id = video.playlist_id
            GROUP BY channel.channel_name
            ORDER BY total_views DESC;'''
    result = pd.read_sql_query(query, engine)
    engine.dispose()
    st.table(result)
elif selection == '8. What are the names of all the channels that have published videos in the year 2022?':
    query = '''SELECT channel.channel_name, video.video_name, video.published_date 
            FROM channel 
            JOIN playlist ON channel.channel_id = playlist.channel_id 
            JOIN video ON playlist.playlist_id = video.playlist_id 
            WHERE EXTRACT(YEAR FROM published_date) = 2022'''
    result = pd.read_sql_query(query, engine)
    engine.dispose()
    st.dataframe(result)
elif selection == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query = '''SELECT channel.channel_name, SEC_TO_TIME(AVG(TIME_TO_SEC(video.duration))) AS duration 
            FROM channel
            JOIN playlist ON channel.channel_id = playlist.channel_id 
            JOIN video ON playlist.playlist_id = video.playlist_id
            GROUP BY channel_name 
            ORDER BY duration DESC'''
    result = pd.read_sql_query(query, engine)
    # Convert the duration column to string representation
    result['duration'] = result['duration'].astype(str)
    engine.dispose()
    st.dataframe(result)
elif selection == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query = '''SELECT channel.channel_name, video.video_name, video.comment_count 
            FROM channel JOIN playlist ON channel.channel_id = playlist.channel_id 
            JOIN video ON playlist.playlist_id = video.playlist_id
            ORDER BY video.comment_count DESC'''
    result = pd.read_sql_query(query, engine)
    engine.dispose()
    st.dataframe(result)