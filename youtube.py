from googleapiclient.discovery import build
import pymongo
import pandas as pd
import mysql.connector as sql
import pymysql
import streamlit as st
from streamlit_option_menu import option_menu
from PIL import Image
import plotly.express as px

api_key = "AIzaSyDeNGdSiAyKWkvHmKZzNZb3yT0_xN3PmAg"
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name, api_version,developerKey = api_key)


def get_channel_details(c_id):
     request=youtube.channels().list(
       part="snippet,contentDetails,statistics",
       id=c_id
)
     channel_response = request.execute()
     
     for i in channel_response['items']:
           data = dict(channel_name=i['snippet']['title'],
                channel_id=i['id'],
                subscribers=i['statistics']['subscriberCount'],
                channel_views=i['statistics']['viewCount'],
                Total_videos=i['statistics']['videoCount'],
                Description=i['snippet']['description'],
                playlist_id=i['contentDetails']['relatedPlaylists']['uploads']
                )
     return data


channel_data = get_channel_details("UCnz-ZXXER4jOvuED5trXfEA")

def get_video_ids(c_id):
    video_ids=[]
    response = youtube.channels().list(id=c_id,
                                    part='contentDetails').execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page = None
    while True:
        video_response = youtube.playlistItems().list(
                                    part='snippet',
                                    playlistId=playlist_id,
                                    maxResults=50,
                                    pageToken=next_page).execute()
        for i in range(len(video_response['items'])):
            video_ids.append(video_response['items'][i]['snippet']['resourceId']['videoId'])
        next_page=video_response.get('nextPageToken')
        if next_page is None:
            break
    return video_ids

            


v_id=get_video_ids('UCnz-ZXXER4jOvuED5trXfEA')


v_id


def get_video_details(V_ID):
    video_data=[]
    for video_id in V_ID:
        v_request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        v_response=v_request.execute()
        for item in v_response["items"]:
                                    data=dict(
                                            Video_Id=item['id'],
                                            Vide_name=item['snippet']['title'],
                                            Tags=item['snippet'].get('tags'),
                                            Description=item['snippet'].get('description'),
                                            PublishedAt=item['snippet']['publishedAt'],
                                            Thumbnail=item['snippet']['thumbnails']['default']['url'],
                                            Duration=item['contentDetails']['duration'],
                                            Views_count=item['statistics'].get('viewCount'),
                                            Likes_count=item['statistics'].get('likeCount'),
                                            Comments_count=item['statistics'].get('commentCount'),
                                            Favorite_Count=item['statistics']['favoriteCount'],
                                            Definition=item['contentDetails']['definition'],
                                            Caption_Status=item['contentDetails']['caption']
                                            )
                                    video_data.append(data)    
    return video_data


v_details = get_video_details(v_id)


v_details


def get_comment_details(co_ids):
    comment_data=[]
    try:
        for co_id  in co_ids:
                            c_request=youtube.commentThreads().list(
                                part="snippet",
                                videoId=co_id,
                                maxResults=50
                            )
                            c_response=c_request.execute()

                            for item in c_response['items']:
                                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                                comment_data.append(data)                         
    except:
        pass

    return comment_data                


c_details = get_comment_details(v_id)


c_details


c_obj=pymongo.MongoClient("mongodb://cindhu10:guvi2024@ac-8q7snle-shard-00-00.bw8cuhe.mongodb.net:27017,ac-8q7snle-shard-00-01.bw8cuhe.mongodb.net:27017,ac-8q7snle-shard-00-02.bw8cuhe.mongodb.net:27017/?ssl=true&replicaSet=atlas-6n6ftf-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
db=c_obj["Youtube_data"]


def channel_details(ch_id):
    c_details=get_channel_details(ch_id)
    vi_id=get_video_ids(ch_id)
    v_details=get_video_details(vi_id)
    com_details=get_comment_details(vi_id)

    collect=db["channel_details"]
    collect.insert_one({"channel_documents":c_details,
                      "video_documents":v_details,"comment_documents":com_details})
    
    return "uploaded successfully"


coll = channel_details("UCnz-ZXXER4jOvuED5trXfEA")
coll


mydb = sql.connect(host="127.0.0.1",
                   user="root@localhost",
                   password="root@123",
                   database= "Youtube",
                   port = "3306"
                  )
mycursor = mydb.cursor(buffered=True)


with st.sidebar:
    selected = option_menu(None, ["Home","Extract & Transform","View"], 
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#33A5FF"},
                                   "icon": {"font-size": "30px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#33A5FF"}})


if selected == "Home":
    col1,col2 = st.columns(2,gap= 'medium')
    col1.markdown("## :blue[Domain] : Social Media")
    col1.markdown("## :blue[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    col1.markdown("## :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    col2.markdown("#   ")
    col2.markdown("#   ")
    col2.markdown("#   ")


if selected == "Extract":
    tab1,tab2 = st.tabs(["$\medium  EXTRACT "])
    
    
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        ch_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')

        if ch_id and st.button("Extract Data"):
            ch_details = get_channel_details(ch_id)
            st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
            st.table(ch_details)

        if st.button("Upload to MongoDB"):
            with st.spinner('Please Wait for it...'):
                c_details = get_channel_details(ch_id)
                v_id = get_video_ids(ch_id)
                vid_details = get_video_details(v_id)
                
                def comments():
                    com_d = []
                    for i in v_id:
                        com_d+= get_comment_details(i)
                    return com_d
                comm_details = comments()

                collections1 = db.channel_details
                collections1.insert_many(c_details)

                collections2 = db.video_details
                collections2.insert_many(vid_details)

                collections3 = db.comments_details
                collections3.insert_many(comm_details)
                st.success("Upload to MongoDB successful !!")
      

    with tab2:     
            st.markdown("#   ")
            st.markdown("### Select a channel to begin Transformation to SQL")
            ch_names = channel_details()  
            user_inp = st.selectbox("Select channel",options= ch_names)
        
    def insert_into_channels():
                    collections = db.channel_details
                    query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
                
                    for i in collections.find({"channel_name" : user_inp},{'_id' : 0}):
                        mycursor.execute(query,tuple(i.values()))
                    mydb.commit()
                
    def insert_into_videos():
            collections1 = db.video_details
            query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            for i in collections1.find({"channel_name" : user_inp},{'_id' : 0}):
                values = [str(val).replace("'", "''").replace('"', '""') if isinstance(val, str) else val for val in i.values()]
                mycursor.execute(query1, tuple(values))
                mydb.commit()

    def insert_into_comments():
            collections1 = db.video_details
            collections2 = db.comments_details
            query2 = """INSERT INTO comments VALUES(%s,%s,%s,%s,%s,%s,%s)"""

            for vid in collections1.find({"channel_name" : user_inp},{'_id' : 0}):
                for i in collections2.find({'Video_id': vid['Video_id']},{'_id' : 0}):
                    mycursor.execute(query2,tuple(i.values()))
                    mydb.commit()

    if st.button("Submit"):
        try:
            insert_into_videos()
            insert_into_channels()
            insert_into_comments()
            st.success("Transformation to MySQL Successful !!")
        except:
            st.error("Channel details already transformed !!")
            

if selected == "View":
    
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT Video_name AS Video_name, channel_name AS Channel_Name
                            FROM videos
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
            mycursor.execute("""SELECT channel_name AS Channel_Name, total_videos AS Total_Videos
                                FROM channels
                                ORDER BY total_videos DESC""")
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df)
            st.write("### :green[Number of videos in each channel :]")
            #st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
            fig = px.bar(df,
                        x=mycursor.column_names[0],
                        y=mycursor.column_names[1],
                        orientation='v',
                        color=mycursor.column_names[0]
                        )
            st.plotly_chart(fig,use_container_width=True)
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, Video_name AS Video_Title, View_count AS Views 
                            FROM videos
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.video_id AS Video_id, Video_name AS Video_Title, b.Total_Comments
                            FROM videos AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comments GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,Video_name AS Title,Like_count AS Like_Count 
                            FROM videos
                            ORDER BY Like_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT Video_name AS Title, Like_count AS Like_count
                            FROM videos
                            ORDER BY Like_count DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, channel_views AS Views
                            FROM channels
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name AS Channel_Name
                            FROM videos
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,
                            AVG(duration)/60 AS "Average_Video_Duration (mins)"
                            FROM videos
                            GROUP BY channel_name
                            ORDER BY AVG(duration)/60 DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Avg video duration for channels :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,Video_id AS Video_ID,Comment_count AS Comments
                            FROM videos
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=mycursor.column_names[1],
                     y=mycursor.column_names[2],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)


