# %%
#Packages
import streamlit as st 
import googleapiclient.discovery 
import plotly.express as px 
import mysql.connector as sql 
import plotly.graph_objects as go 
import pandas as pd 
import pymongo 
import isodate 
from datetime import datetime

# %%
#api connection
api_key = "AIzaSyAmZhOf0Wj7RiP7KtUnlXZGGjWdNrUcBVI"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name,api_version,developerKey=api_key)

# %%
#sql connection
mydb = sql.connect(host="localhost",user="root",password="root@123",port=3306,database="youtube")
cursor = mydb.cursor(buffered=True)

# %%
#mongodb connection
mongo_con = pymongo.MongoClient("mongodb://cindhu10:guvi2024@ac-8q7snle-shard-00-00.bw8cuhe.mongodb.net:27017,ac-8q7snle-shard-00-01.bw8cuhe.mongodb.net:27017,ac-8q7snle-shard-00-02.bw8cuhe.mongodb.net:27017/?ssl=true&replicaSet=atlas-6n6ftf-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
print(mongo_con)
# %%
st.set_page_config(
                   page_title="Youtube Data Harvesting and Warehousing",
                   page_icon="▶️",
                   layout="wide",
                   initial_sidebar_state="auto"
)

# %%
st.title("YouTube Data Harvesting and Warehousing")
user_input = st.text_input("Enter Channel Id:")

# %%
#Channel Data
def get_channel_details(c_id):
     data =[]
     request=youtube.channels().list(
       part="snippet,contentDetails,statistics",
       id=c_id
)
     channel_response = request.execute()
     
     for i in channel_response['items']:
            ch_data = dict(Channel_name=i['snippet']['title'],
                Channel_id=i['id'],
                subscribers=i['statistics']['subscriberCount'],
                Views=i['statistics']['viewCount'],
                Total_videos=i['statistics']['videoCount'],
                Description=i['snippet']['description'],
                playlist_id=i['contentDetails']['relatedPlaylists']['uploads']
                )
            data.append(ch_data)
     return data


# %%
#Video IDS
def get_video_ids(c_id):
    video_ids=[]
    response1 = youtube.channels().list(id=c_id,
                                    part='contentDetails').execute()
    playlist_id=response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page = None
    while True:
                                        
        response2 = youtube.playlistItems().list( part='snippet', 
                                           playlistId=playlist_id, 
                                           maxResults=50,
                                           pageToken=next_page).execute()
        
        for i in range(len(response2['items'])):
            video_ids.append(response2['items'][i]['snippet']['resourceId']['videoId'])
        next_page=response2.get('nextPageToken')
        if next_page is None:
            break
    return video_ids

# %%
def get_video_details(v_ids):
    video_datas=[]
    for video_id in v_ids:
      request=youtube.videos().list(part="snippet, ContentDetails, statistics",id=video_id)
      response=request.execute()
      tag=response['items'][0]['snippet'].get("tags",[])
      t=",".join(tag)
      datetime_str = response['items'][0]['snippet']['publishedAt']
      try:
        datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')
      except ValueError:
        datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ')

      Duration=response['items'][0]['contentDetails']['duration']
      duration = isodate.parse_duration(Duration)
      user = str(duration).replace('0:0:0', '')
      for item in response["items"]:
        data=dict(Channel_name=item['snippet']['channelTitle'], 
                  Channel_id=item['snippet']['channelId'],
                  Video_Id=item['id'], Title=item['snippet']['title'],
                  Tags=t,
                  Thumbnail=item['snippet']['thumbnails']['default']['url'],
                  Description=item['snippet'].get('description'), 
                  Published_Date=datetime_obj, Duration=user,
                  Views=item['statistics']['viewCount'], 
                  Likes=item['statistics'].get('likeCount'),
                  Comments=item['statistics'].get('commentCount'),
                  Favorite_Count=item['statistics'].get('favoriteCount'),
                  Caption_Status=item['contentDetails']['caption'])
        video_datas.append(data)
    return video_datas


# %%
#comments data
def get_comment_details(v_ids):
    comment_data=[]
    try:
        next_page = None
        for co_id  in v_ids:
                            
            c_request=youtube.commentThreads().list(
                                part="snippet",
                                videoId=co_id,
                                maxResults=100,
                                pageToken = next_page
                            )
            c_response=c_request.execute()

            for item in c_response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                          Channel_id=item['snippet']['channelId'],
                          Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                          Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_data.append(data) 
            next_page = c_response.get('nextPageToken')  
            if next_page is None:
                break                      
    except:
        pass

    return comment_data            

# %%
#Transfer to MongoDB
def channel(user_input):
    db = mongo_con['Youtube_data']
    collection1=db['channel']
    details_list = get_channel_details(user_input)
    for details in details_list:
        collection1.insert_one(details)   


def video(user_input):
    db = mongo_con['Youtube_data']
    collection2=db['video']
    id = get_video_ids(user_input)
    video_details = get_video_details(id)
    #collection2.insert_many(video_details)
    for details1 in video_details:
        collection2.insert_one(details1)     

def comments(user_input):
    db = mongo_con['Youtube_data']
    collection3=db['comments']
    id = get_video_ids(user_input)
    comment_details = get_comment_details(id)
    collection3.insert_many(comment_details)

# %%
#function table_exists
def table_exists_channel(cursor,channel):
    cursor.execute("SHOW TABLES LIKE %s", (channel,))
    return cursor.fetchone() is not None

def table_exists_video(cursor,video):
    cursor.execute("SHOW TABLES LIKE %s", (video,))
    return cursor.fetchone() is not None

def table_exists_comments(cursor,comments):
    cursor.execute("SHOW TABLES LIKE %s", (comments,))
    return cursor.fetchone() is not None

# %%
#function migrate to mysql
def migrate_channel(user_input):    
    db = mongo_con['Youtube_data']
    col1 = db['channel']
    if not table_exists_channel(cursor, 'channel'):
        cursor.execute("""
            CREATE TABLE channel (
                Channel_id VARCHAR(255) PRIMARY KEY,
                Channel_name VARCHAR(255),
                Description TEXT,
                playlist_id VARCHAR(255),
                subscribers INT,
                Views_count INT,
                Total_videos INT
            )""")
    
    try:
        cursor.execute("SELECT COUNT(*) FROM channel WHERE Channel_id = %s", (user_input,))
        count = cursor.fetchone()[0]
        if count > 0:
            return False
        for document in col1.find({"Channel_id": user_input}, {"_id": 0}):
            cursor.execute("""
                INSERT INTO channel (
                    Channel_id, Channel_name, Description, playlist_id, subscribers, Views_count,
                    Total_videos
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)""",                 
                (
                    document.get("Channel_id"), document.get("Channel_name"), document.get("Description"), 
                    document.get("playlist_id"), document.get("subscribers"), document.get("Views"), 
                    document.get("Total_videos")
                ))
        mydb.commit()
        return True
    except Exception as e:
        print("Error occurred during migration:", e)
        mydb.rollback() 
        return False
    
def migrate_video(user_input):    
    db = mongo_con['Youtube_data']
    col2 = db['video']
    if not table_exists_video(cursor, 'video'):
         cursor.execute("""                              
                    CREATE TABLE video (
                    Channel_name VARCHAR(255),
                    Channel_id VARCHAR(255),
                    Video_Id VARCHAR(255),
                    Title VARCHAR(255),
                    Thumbnail VARCHAR(255),
                    Description TEXT,
                    Published_Date DATETIME,
                    Duration VARCHAR(50), 
                    Views_count INT,
                    Likes_count INT,
                    Comments INT,
                    Favorite_Count INT,
                    Caption_Status VARCHAR(5)                                                                 
                )
            """)
    
    try:
        cursor.execute("SELECT COUNT(*) FROM video WHERE Channel_Id = %s", (user_input,))
        count = cursor.fetchone()[0]
        if count > 0:
            return False
        
        for document in col2.find({"Channel_id": user_input}, {"_id": 0}):
            cursor.execute("""
                INSERT INTO video (                   
                    Channel_name, Channel_id, Video_Id, Title, Thumbnail, Description, Published_Date, 
                    Duration, Views_count, Likes_count, Comments, Favorite_Count, Caption_Status                    
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                document.get("Channel_name"),
                document.get("Channel_id"),
                document.get("Video_Id"),
                document.get("Title"),
                document.get("Thumbnail"),
                document.get("Description"),
                document.get("Published_Date"),                
                document.get("Duration"),
                document.get("Views"),
                document.get("Likes"),
                document.get("Comments"),
                document.get("Favorite_Count"),
                str(document.get("Caption_Status")).lower()  # Convert boolean to string ("true"/"false")
            ))
                
        mydb.commit()
        return True
    except Exception as e:
        print("Error occurred during migration:", e)
        mydb.rollback() 
        return False   
 
# Migrate comments function
def migrate_comments(user_input):
    db = mongo_con['Youtube_data']  
    col3 = db['comments']
    if not table_exists_comments(cursor, 'comments'):    
        cursor.execute("""
            CREATE TABLE comments (
                Comment_Id VARCHAR(255),
                Channel_id VARCHAR(255),
                Comment_Text TEXT,
                Comment_Author VARCHAR(255), 
                Comment_Published DATETIME
            )
        """)
    try:
        cursor.execute("SELECT COUNT(*) FROM comments WHERE Channel_id = %s", (user_input,))
        count = cursor.fetchone()[0]
        if count > 0:
           return False
        for document in col3.find({"Channel_id": user_input}, {"_id": 0}):
            Comment_Published = datetime.strptime(document.get("Comment_Published"), '%Y-%m-%dT%H:%M:%SZ')
            cursor.execute("""
                INSERT INTO comments (
                    Comment_Id, Channel_id, Comment_Text, Comment_Author, Comment_Published
                ) VALUES (
                    %s, %s, %s, %s, %s
                )
            """, (
                document.get("Comment_Id"),
                document.get("Channel_id"),
                document.get("Comment_Text"),
                document.get("Comment_Author"),
                Comment_Published.strftime('%Y-%m-%d %H:%M:%S')
            ))
        mydb.commit()
        return True
    except Exception as e:
        print("Error occurred during comments migration:", e)
        mydb.rollback() 
        return False



# %%
#function queries
def Query1(): 
      query1=""" SELECT Channel_name, Title FROM video"""
      cursor.execute(query1)
      r1=cursor.fetchall()
      c1=cursor.column_names
      df1=pd.DataFrame(r1,columns=c1)
      st.write(df1)



def Query2():
      query2="""SELECT Channel_name , count(Channel_name) as No_of_videos FROM video GROUP BY Channel_name ORDER BY No_of_videos DESC
"""
      cursor.execute(query2)
      r2=cursor.fetchall()
      c2=cursor.column_names
      df2=pd.DataFrame(r2,columns=c2)
      st.write(df2)
      st.write("### :green[Number of videos in each channel :]")
      fig = go.Figure(go.Bar(x=df2['Channel_name'],y=df2['No_of_videos'] ))
      fig.update_layout(
        title="No. of videos per channel",
        xaxis_title="Channel_name",
        yaxis_title="No. of videos",
        yaxis=dict(range=[0, 350]),
        margin=dict(l=100, r=20, t=30, b=20), 
        height=400  
        )
      st.plotly_chart(fig, use_container_width=True)
      
      fig=px.pie(df2,names="Channel_name",values="No_of_videos")
      fig.update_layout(
        title="No.of videos per channel" )
      st.plotly_chart(fig)
      

# %%
def Query3():
      query3="""SELECT Channel_name,Title,Views_count FROM video ORDER BY Views_count DESC LIMIT 10"""
      cursor.execute(query3)
      r3=cursor.fetchall()
      c3=cursor.column_names
      df3=pd.DataFrame(r3,columns=c3)
      st.write(df3)
      st.write("### :green[ Top 10 most viewed videos :]")
      fig = go.Figure(go.Bar(y=df3['Title'],x=df3['Views_count'],orientation='h' ))
      fig.update_layout(
        title="Views_count per Video",
        xaxis_title="Views_count",
        yaxis_title="Video Title",
        margin=dict(l=100, r=20, t=30, b=20), 
        height=400  
        )
      st.plotly_chart(fig, use_container_width=True)

# %%
def Query4():
      query4="""SELECT Title AS Video_names ,Comments AS comment_count FROM video"""
      cursor.execute(query4)
      r4=cursor.fetchall()
      c4=cursor.column_names
      df4=pd.DataFrame(r4,columns=c4)
      st.write(df4)

def Query5():
      query5="""SELECT Channel_name,Title AS Video_names ,Likes_count FROM video ORDER BY Likes_count DESC"""
      cursor.execute(query5)
      r5=cursor.fetchall()
      c5=cursor.column_names
      df5=pd.DataFrame(r5,columns=c5)
      st.write(df5)

def Query6():
      st.write("YouTube has hidden the dislike count")
      query6="""SELECT Channel_name,Title AS Video_names ,Likes_count FROM video"""
      cursor.execute(query6)
      r6=cursor.fetchall()
      c6=cursor.column_names
      df6=pd.DataFrame(r6,columns=c6)
      st.write(df6)


# %%
def Query7():
      query7="""SELECT Channel_name, Views_count FROM channel"""
      cursor.execute(query7)
      r7=cursor.fetchall()
      c7=cursor.column_names
      df7=pd.DataFrame(r7,columns=c7)
      st.write(df7)
      st.write("### :green[View count for each channel :]")
      fig = go.Figure(go.Bar(y=df7['Channel_name'],x=df7['Views_count'],orientation='h' ))
      fig.update_layout(
        title="Views_count per Channel",
        xaxis_title="Views_count",
        yaxis_title="Channel_name",
        xaxis=dict(range=[0, 300000000]),
        margin=dict(l=100, r=20, t=30, b=20), 
        height=400  
        )
      st.plotly_chart(fig, use_container_width=True)

# %%
def Query8():
      query8="""SELECT Channel_name, count(Channel_name) AS Total_videos FROM video WHERE YEAR(Published_Date)=2022 GROUP by Channel_name"""
      cursor.execute(query8)
      r8=cursor.fetchall()
      c8=cursor.column_names
      df8=pd.DataFrame(r8,columns=c8)
      st.write(df8)
      st.write("### :green[No. of videos published in 2022 for each channel :]")
      fig=px.pie(df8,names="Channel_name",values="Total_videos")
      fig.update_layout(
        title="No.of videos published in 2022 " )
      st.plotly_chart(fig)

# %%
def Query9():
      query9="""SELECT Channel_name,avg(TIME_TO_SEC(Duration)) AS Duration_in_seconds FROM video GROUP BY Channel_name"""
      cursor.execute(query9)
      r9=cursor.fetchall()
      c9=cursor.column_names
      df9=pd.DataFrame(r9,columns=c9)
      st.write(df9)
      st.write("### :green[Average video duration of each channel  :]")
      fig = go.Figure(go.Pie(labels=df9['Channel_name'], values=df9['Duration_in_seconds'], hole=0.5))
      fig.update_layout(
        title="Average video duration of each channel" )
      st.plotly_chart(fig)

def Query10():
      query10="""SELECT Channel_name, Title AS Video_names ,Comments AS comment_count FROM video ORDER BY Comments DESC"""
      cursor.execute(query10)
      r10=cursor.fetchall()
      c10=cursor.column_names
      df10=pd.DataFrame(r10,columns=c10)
      st.write(df10)

# %%

tab1, tab2, tab3, tab4 = st.tabs([r"$\large COLLECT DATA $", r"$\large TRANSFER $", r"$\large MIGRATE  $", r"$\large VIEW $" ])
with tab1:
    st.header("Collection of data page")
    if st.button("Collect Channel Details"):
        collection1=get_channel_details(user_input)
        st.write(' Extracted data from channel ')
        st.table(collection1)
    
    if st.button("Collect Video Ids"):
        id=get_video_ids(user_input)
        vi_df=pd.DataFrame(id)
        st.write(vi_df)
    
    if st.button("Collect Video Details"):
        id=get_video_ids(user_input)
        video_details=get_video_details(id)
        v_df=pd.DataFrame(video_details)
        st.write(' Extracted data from videos ')
        st.write(v_df)

    if st.button("Collect Comment Details"):
        id=get_video_ids(user_input)
        comment_details=get_comment_details(id)
        c_df=pd.DataFrame(comment_details)
        st.write(' Extracted data from comments ')
        st.write(c_df)


# %%
with tab2:
    st.header("Transfer datas to mongoDB")
    if st.button("Transfer Channel to mongoDB"):
        ids=[]
        for ids_c in mongo_con['Youtube_data']['channel'].find({},{"_id":0,"Channel_id":1}):
            ids.append(ids_c['Channel_id'])

        if user_input in ids:
           st.error("Channel Id already exists")
        else:
            tc=channel(user_input)
            st.success("Channel details uploaded to mongodb successfully!!!")

    if st.button("Transfer Videos to mongodb"):
        ids_v=mongo_con['Youtube_data']['video'].distinct("Channel_id")
        if user_input in ids_v:
           st.error("Video details already exists")
        else:
            tv=video(user_input)
            st.success("Video details uploaded to mongodb successfully!!!")
       
        
    if st.button("Transfer Comments to mongodb"):
        com_d= mongo_con['Youtube_data']['comment'].distinct("Channel_id")
        if user_input in com_d:
           st.error("comment details already exists")
        else:
            tco=comments(user_input)
            st.success("Comment details uploaded to mongodb successfully!!!")

with tab3:
   st.header("Migrate datas to MySQL page")
   if st.button("Migrate Channel"):
          mc=migrate_channel(user_input)
          if mc:
            st.success("Channel details uploaded to mySQL successfully!!!")
          else:
           st.error("Details already transformed!!!")

   if st.button("Migrate Video"):
          mv=migrate_video(user_input)
          if mv:
            st.success("Video details uploaded to MySQL successfully!!!")
          else:
           st.error("Details already transformed!!!")

   if st.button("Migrate comments"):
          mco=migrate_comments(user_input)
          if mco:
            st.success("Comments detail uploaded to MySQL successfully!!!")
          else:
           st.error("Details already transformed!!!")

# %%
with tab4:    
   st.header("Queries page")
   st.write("Select Queries and view the result")
   sidebar_options = ["1. What are the names of all the videos and their corresponding channels?",
                      "2. Which channels have the most number of videos, and how many videos do they have?",
                      "3. What are the top 10 most viewed videos and their respective channels?",
                      "4. How many comments were made on each video, and what are their corresponding video names?",
                      "5. Which videos have the highest number of Likes_count, and what are their corresponding channel names?",
                      "6. What is the total number of Likes_count and disLikes_count for each video, and what are their corresponding video names?",
                      "7. What is the total number of Views_count for each channel, and what are their corresponding channel names?",
                      "8. What are the names of all the channels that have published videos in the year 2022?",                    
                      "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                      "10. Which videos have the highest number of comments, and what are their corresponding channel names?"]
   selection = st.selectbox("QUERIES", sidebar_options)

   if selection == "1. What are the names of all the videos and their corresponding channels?":
        Query1()

   if selection == "2. Which channels have the most number of videos, and how many videos do they have?":
        Query2()

   if selection == "3. What are the top 10 most viewed videos and their respective channels?":
       Query3()

   if selection == "4. How many comments were made on each video, and what are their corresponding video names?":
       Query4()

   if selection == "5. Which videos have the highest number of Likes_count, and what are their corresponding channel names?":
       Query5()

   if selection == "6. What is the total number of Likes_count and disLikes_count for each video, and what are their corresponding video names?":
       Query6()

   if selection == "7. What is the total number of Views_count for each channel, and what are their corresponding channel names?":
       Query7()

   if selection == "8. What are the names of all the channels that have published videos in the year 2022?":
       Query8()

   if selection == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
       Query9()

   if selection == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
       Query10()


