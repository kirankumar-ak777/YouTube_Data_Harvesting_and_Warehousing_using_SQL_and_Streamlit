# Import necessary libraries
from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
import datetime

#Youtube API Key Connection:
def Api_Conn():
    # Your YouTube API key
    Api_Id="AIzaSyAwwgh7FuCnAZepmJ9u4nvfoJkHOeCjgDk"

    # YouTube API service name and version
    api_serv_name="Youtube"
    api_version="v3"

    # Build the YouTube API service
    youtube=build(api_serv_name,api_version,developerKey=Api_Id)

    return youtube
# Call the function to get the YouTube API connection
youtube = Api_Conn()

#To get Youtube Channel Informations
def get_channel_info(Channel_id):
    # Make a call to the YouTube API to get channel information
    call=youtube.channels().list(
    part="snippet,ContentDetails,statistics",
    id=Channel_id
    )
    # Execute the API call and get the response
    res=call.execute()

    for i in res['items']:
        # Extract relevant information from the API response
        data = dict(Channel_Name=i["snippet"]["title"],
                    Channel_Id=i["id"],
                    Subscription_Count=i["statistics"]["subscriberCount"],
                    Views=i["statistics"]["viewCount"],
                    Channel_Description=i["snippet"]["description"],
                    Total_Videos=i["statistics"]["videoCount"],
                    Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data


# To Get Youtube Channels Video Id's:
def get_vid_id(Channel_id):
    vide_id=[] # List to store video ids

    # Get the uploads playlistId for the given channel
    res=youtube.channels().list(id=Channel_id,
                            part="contentDetails").execute()
    Playlist_Id=res["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    nex_page =None

    # Loop to retrieve video ids from the uploads playlist
    while True:
        # Get playlist items (videos) from the current page
        res1 = youtube.playlistItems().list(
                                        part="snippet", 
                                        playlistId=Playlist_Id,
                                        maxResults=50,
                                        pageToken=nex_page).execute()
        
        # Extract video ids from the current page and add them to the list
        for i in range(len(res1["items"])):
            vide_id.append(res1["items"][i]["snippet"]["resourceId"]["videoId"])
        nex_page=res1.get("nextPageToken")    # Get the token for the next page
        
        # Break the loop if there is no next page
        if nex_page is None:
            break
    return vide_id


# To get Video Information
def get_vid_info(Video_ids):

    vid_data=[] # List to store video data

    # Loop through each video ID
    for Video_id in Video_ids:
        req=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=Video_id
        )
        resp=req.execute()
    
        # Loop through each item in the response (usually one item per video)
        for item in resp["items"]:
            # Extract relevant information and create a dictionary
            data=dict(Channel_Name=item["snippet"]["channelTitle"],
                    Channel_Id=item["snippet"]["channelId"],
                    Video_Id=item["id"],
                    Title=item["snippet"]["title"],
                    Tags=item["snippet"].get("tags"),
                    Thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                    Description=item["snippet"].get("description"),
                    Published_Date=item["snippet"]["publishedAt"],
                    Duration=item["contentDetails"]["duration"],
                    Views=item["statistics"].get("viewCount"),
                    Likes=item["statistics"].get("likeCount"),
                    Comments=item["statistics"].get("commentCount"),
                    Favorite_Count=item["statistics"]["favoriteCount"],
                    Definition=item["contentDetails"]["definition"],
                    Caption_Status=item["contentDetails"]["caption"]
                    )
            # Append the dictionary to the vid_data list
            vid_data.append(data)
    # Return the list of video data
    return vid_data

#To get a youtube channels videos comments details
def get_comm_info(Video_ids):
    # List to store comment data
    Comm_data = []

    try:
        # Loop through each video ID
        for Video_id in Video_ids:
            # Request comment information from YouTube API
            requ = youtube.commentThreads().list(
                part="snippet",
                videoId=Video_id,
                maxResults=50
            )
            resp = requ.execute()

            # Loop through each item in the response (usually one item per comment thread)
            for i in resp["items"]:
                # Extract relevant information and create a dictionary
                data = dict(
                    Comme_id=i['snippet']["topLevelComment"]["id"],
                    Vd_id=i["snippet"]["topLevelComment"]["snippet"]["videoId"],
                    Comm_text=i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    Commentor=i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    Comm_publish=i["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                )
                # Append the dictionary to the Comm_data list
                Comm_data.append(data)

    except Exception as e:
        # Handle exceptions (you might want to log or print the exception)
        print(f"Exception: {e}")

    # Return the list of comment data
    return Comm_data


# To Get Youtube Channels Playlist Details
def get_playlist_det(Channel_id):
    # Initialize next page token as None
    nex_page_token = None

    # List to store all playlist data
    All_data = []

    # Continue fetching playlists until there are no more pages
    while True:
        # Request playlist information from YouTube API
        req = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=Channel_id,
            maxResults=50,
            pageToken=nex_page_token
        )
        res = req.execute()

        # Loop through each playlist item in the response
        for j in res["items"]:
            # Extract relevant information and create a dictionary
            data = dict(
                Playlist_Id=j["id"],
                Title=j["snippet"]["title"],
                Channel_Id=j["snippet"]["channelId"],
                Channel_Name=j["snippet"]["channelTitle"],
                PublishedAt=j["snippet"]["publishedAt"],
                Video_count=j["contentDetails"]["itemCount"]
            )
            # Append the dictionary to the All_data list
            All_data.append(data)

        # Get the next page token
        nex_page_token = res.get("nextPageToken")

        # Break the loop if there is no next page
        if nex_page_token is None:
            break

    # Return the list of playlist data
    return All_data

#To Connect MongoDB
client=pymongo.MongoClient("mongodb://localhost:27017")
db = client["Youtube_Details"]


#Function for Full details for youtube channels
def full_Channel_Details(Channel_id):
    # Get channel information
    Channel_details = get_channel_info(Channel_id)
    
    # Get video IDs associated with the channel
    Video_IDS = get_vid_id(Channel_id)
    
    # Get detailed information about the videos
    Video_Details = get_vid_info(Video_IDS)
    
    # Get comments information for the videos
    Comment_Details = get_comm_info(Video_IDS)
    
    # Get playlist details for the channel
    Playlist_Details = get_playlist_det(Channel_id)

    # Insert the gathered information into the MongoDB collection
    Coll1 = db["Channel_Details"]
    Coll1.insert_one({
        "Channel_Information": Channel_details,
        "Playlist_Information": Playlist_Details,
        "Video_Information": Video_Details,
        "Comment Details": Comment_Details
    })

    # Return a message indicating successful upload
    return "Upload Completed Successfully"


# To Create Youtube Channels Table in MySQL
def channels_table(Channel_name):
    # Connect to MySQL database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kiran",
        database="youtube_harvest_warehousing", 
        port="3306"
    )

    cursor = mydb.cursor()

    
    # Create channels table if it does not exist
    create_query = """CREATE TABLE IF NOT EXISTS channels (
                        Channel_Name VARCHAR(100),
                        Channel_Id VARCHAR(100) PRIMARY KEY,
                        Subscription_Count INT,
                        Views INT,
                        Total_Videos INT,
                        Channel_Description TEXT,
                        Playlist_Id VARCHAR(100)
                    )"""
    cursor.execute(create_query)
    mydb.commit()

    # Retrieve channel information from MongoDB
    One_chan_name = []
    db = client["Youtube_Details"]
    Coll1 = db["Channel_Details"]

    for channel_data in Coll1.find({"Channel_Information.Channel_Name":Channel_name}, {"_id": 0}):
        One_chan_name.append(channel_data["Channel_Information"])

    df_one_chan = pd.DataFrame(One_chan_name)

    # Insert channel information into MySQL channels table
    for index, row in df_one_chan.iterrows():
        insert_query = """INSERT INTO channels (
                            Channel_Name,
                            Channel_Id,
                            Subscription_Count,
                            Views,
                            Total_Videos,
                            Channel_Description,
                            Playlist_Id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
 
        values = (row["Channel_Name"],
                  row["Channel_Id"],
                  row["Subscription_Count"],
                  row["Views"],
                  row["Total_Videos"],
                  row["Channel_Description"],
                  row["Playlist_Id"])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()

        except:
            alr = f"Your Provided Channel Name {Channel_name} is Already Exists"

            return alr

# To Create youtube channels Playlist Table in MySQL
def playlists_table(Channel_name):
    # Connect to MySQL database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kiran",
        database="youtube_harvest_warehousing",
        port="3306"
    )

    cursor = mydb.cursor()

    # Create Playlists table
    create_query = """CREATE TABLE IF NOT EXISTS Playlists(
                        Playlist_Id VARCHAR(100) PRIMARY KEY,
                        Title VARCHAR(100),
                        Channel_Id VARCHAR(100),
                        Channel_Name VARCHAR(100),
                        PublishedAt TIMESTAMP,
                        Video_count INT
                    )"""
    cursor.execute(create_query)
    mydb.commit()

    # Retrieve playlist information from MongoDB
    One_playlist = []
    db = client["Youtube_Details"]
    Coll1 = db["Channel_Details"]

    for channel_data in Coll1.find({"Channel_Information.Channel_Name":Channel_name}, {"_id": 0}):
        One_playlist.append(channel_data["Playlist_Information"][0])

    df_one_playlist = pd.DataFrame(One_playlist)

    # Insert playlist information into MySQL Playlists table
    for index, row in df_one_playlist.iterrows():
        insert_query = """INSERT IGNORE INTO Playlists(
                            Playlist_Id,
                            Title,
                            Channel_Id,
                            Channel_Name,
                            PublishedAt,
                            Video_count
                        ) 
                        VALUES(%s, %s, %s, %s, %s, %s)"""

        values = (row["Playlist_Id"],
                  row["Title"],
                  row["Channel_Id"],
                  row["Channel_Name"],
                  row["PublishedAt"],
                  row["Video_count"])

        cursor.execute(insert_query, values)
        mydb.commit()


# To Create a Youtube Videos Table in MySQL
def videos_table(Channel_name):
    # Connect to MySQL database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kiran",
        database="youtube_harvest_warehousing",
        port="3306"
    )

    cursor = mydb.cursor()

    # Create videos table
    create_query = '''CREATE TABLE IF NOT EXISTS videos(
                        Channel_Name VARCHAR(150),
                        Channel_Id VARCHAR(100),
                        Video_Id VARCHAR(50) PRIMARY KEY, 
                        Title VARCHAR(150), 
                        Tags TEXT,
                        Thumbnail VARCHAR(225),
                        Description TEXT, 
                        Published_Date TIMESTAMP,
                        Duration TIME, 
                        Views INT, 
                        Likes INT,
                        Comments INT,
                        Favorite_Count INT, 
                        Definition VARCHAR(50), 
                        Caption_Status VARCHAR(50) 
                    )'''
    cursor.execute(create_query)             
    mydb.commit()

    # Retrieve video information from MongoDB
    One_Videos = []
    db = client["Youtube_Details"]
    Coll1 = db["Channel_Details"]

    for channel_data in Coll1.find({"Channel_Information.Channel_Name":Channel_name}, {"_id": 0}):
        One_Videos.append(channel_data["Video_Information"])

    df_one_Videos = pd.DataFrame(One_Videos[0])

    for index, row in df_one_Videos.iterrows():
        # Convert the list to a string
        tags_str = ','.join(row['Tags']) if isinstance(row['Tags'], list) else row['Tags']
        def durationtoint(time_str):
        # Check if the time string is in ISO 8601 duration format (e.g., PT1H25M30S)
            if time_str.startswith('PT'):
                time_str = time_str[2:]  # Remove the 'PT' prefix
                total_seconds = 0

                # Iterate through characters in the time string
                current_number = ''
                for char in time_str:
                    if char.isdigit():
                        current_number += char
                    elif char == 'H':
                        total_seconds += int(current_number) * 3600
                        current_number = ''
                    elif char == 'M':
                        total_seconds += int(current_number) * 60
                        current_number = ''
                    elif char == 'S':
                        total_seconds += int(current_number)
                        current_number = ''

                # Convert total seconds to HH:MM:SS format
                hours, remainder = divmod(total_seconds, 3600)
                minutes, seconds = divmod(remainder, 60)

                return f"{hours:02}:{minutes:02}:{seconds:02}"
            else:
                # Handle the original format with hours, minutes, and seconds
                components = time_str.split()
                hours, minutes, seconds = 0, 0, 0

                for component in components:
                    if 'h' in component:
                        hours = int(component[:-1])
                    elif 'm' in component:
                        minutes = int(component[:-1])
                    elif 's' in component:
                        seconds = int(component[:-1])

                return f"{hours:02}:{minutes:02}:{seconds:02}"
        
        insert_query = '''
            INSERT IGNORE INTO videos (Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail,
                                Description, Published_Date, Duration, Views, Likes,
                                Comments, Favorite_Count, Definition, Caption_Status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Video_Id'],
            row['Title'],
            tags_str,
            row['Thumbnail'],
            row['Description'],
            row['Published_Date'],
            durationtoint(row['Duration']),
            row['Views'],
            row['Likes'],
            row['Comments'],
            row['Favorite_Count'],
            row['Definition'],
            row['Caption_Status']
        )

        cursor.execute(insert_query, values)
        # Commit the changes after the loop
        mydb.commit()


# To create a Youtube Channels Comments_Table in MySQL
def comments_table(Channel_name):

    # Connect to MySQL database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kiran",
        database="youtube_harvest_warehousing",
        port="3306"
    )

    cursor = mydb.cursor()

    # Create Comments table
    create_query = """CREATE TABLE IF NOT EXISTS Comments(
                        Comme_id VARCHAR(100) PRIMARY KEY,
                        Vd_id VARCHAR(100),
                        Comm_text TEXT,
                        Commentor VARCHAR(200),
                        Comm_publish TIMESTAMP
                    )"""

    cursor.execute(create_query)
    mydb.commit()

    # Retrieve comment information from MongoDB
    One_Comment = []
    db = client["Youtube_Details"]
    Coll1 = db["Channel_Details"]

    for channel_data in Coll1.find({"Channel_Information.Channel_Name":Channel_name}, {"_id": 0}):
        One_Comment.append(channel_data["Comment Details"])

    df_one_Comment = pd.DataFrame(One_Comment[0])

    for index, row in df_one_Comment.iterrows():
        insert_query = """INSERT IGNORE INTO Comments(Comme_id,
                                                        Vd_id,
                                                        Comm_text,
                                                        Commentor,
                                                        Comm_publish)
                                            VALUES(%s, %s, %s, %s, %s)"""

        Values = (
            row["Comme_id"],
            row["Vd_id"],
            row["Comm_text"],
            row["Commentor"],
            row["Comm_publish"]
        )

        cursor.execute(insert_query, Values)
        mydb.commit()


def tables(One_Channel):
    # Call functions to create tables
    exists =channels_table(One_Channel)
    if exists:
        return exists
    else:
        playlists_table(One_Channel)
        videos_table(One_Channel)
        comments_table(One_Channel)

        return "Tables Created Successfully"


def show_channels_table():
    # Initialize an empty list to store channel information
    Channel_Det = []
    
    # Access the MongoDB collection
    db = client["Youtube_Details"]
    Coll1 = db["Channel_Details"]
    
    # Retrieve channel information from the collection
    for channel_data in Coll1.find({}, {"_id": 0, "Channel_Information": 1}):
        Channel_Det.append(channel_data["Channel_Information"])
    
    # Convert the list to a DataFrame and display it using Streamlit
    df = st.dataframe(Channel_Det)

    return df

def show_playlists_table():
    # Initialize an empty list to store playlist information
    playlist_Det = []

    # Access the MongoDB collection
    db = client["Youtube_Details"]
    Coll1 = db["Channel_Details"]

    # Retrieve playlist information from the collection
    for play_data in Coll1.find({}, {"_id": 0, "Playlist_Information": 1}):
        for i in range(len(play_data["Playlist_Information"])):
            playlist_Det.append(play_data["Playlist_Information"][i])

    # Convert the list to a DataFrame and display it using Streamlit
    df1 = st.dataframe(playlist_Det)

    return df1


def show_vidoes_table():
    # Initialize an empty list to store video information
    V_Det = []

    # Access the MongoDB collection
    db = client["Youtube_Details"]
    Coll1 = db["Channel_Details"]

    # Retrieve video information from the collection
    for v_data in Coll1.find({}, {"_id": 0, "Video_Information": 1}):
        for i in range(len(v_data["Video_Information"])):
            V_Det.append(v_data["Video_Information"][i])

    # Convert the list to a DataFrame and display it using Streamlit
    df2 = st.dataframe(V_Det)

    return df2


def show_comments_table():
    # Initialize an empty list to store comments information
    Comments_Det = []

    # Access the MongoDB collection
    db = client["Youtube_Details"]
    Coll1 = db["Channel_Details"]

    # Retrieve comments information from the collection
    for Comm_data in Coll1.find({}, {"_id": 0, "Comment Details": 1}):
        for i in range(len(Comm_data["Comment Details"])):
            Comments_Det.append(Comm_data["Comment Details"][i])

    # Convert the list to a DataFrame and display it using Streamlit
    df3 = st.dataframe(Comments_Det)

    return df3


# Streamlit part

# Sidebar displaying project information and skill takeaway
with st.sidebar:
    st.title(":rocket: [YOUTUBE DATA HARVESTING AND WAREHOUSING] :rocket:")
    st.header(":large_blue_circle: [Skill Takeaway] :large_blue_circle:")
    st.caption("<span style='color: #3498db;'><b>:small_blue_diamond: Python Scripting</b></span>", unsafe_allow_html=True)
    st.caption("<span style='color: #e74c3c;'><b>:small_blue_diamond: Data Collection</b></span>", unsafe_allow_html=True)
    st.caption("<span style='color: #2ecc71;'><b>:small_blue_diamond: MongoDB</b></span>", unsafe_allow_html=True)
    st.caption("<span style='color: #f39c12;'><b>:small_blue_diamond: API Integration</b></span>", unsafe_allow_html=True)
    st.caption("<span style='color: #9b59b6;'><b>:small_blue_diamond: Data Management using MongoDB and SQL</b></span>", unsafe_allow_html=True)


# Text input for entering the Channel ID
# Set the style for the text input
text_input_style = """
    <style>
        input[type="text"] {
            font-size: 18px;
            color: #3498db;
            padding: 10px;
            border: 2px solid #3498db;
            border-radius: 5px;
            outline: none;
            margin-bottom: 10px;
        }
    </style>
"""

# Apply the style
st.markdown(text_input_style, unsafe_allow_html=True)

# Display the text input with an impressive label
channel_id = st.text_input("🚀 Enter the Channel ID", key="channel_input")


# Button to trigger data collection and storage
st.markdown("""
    <style>
        div[data-testid="stButton"] button {
            background-color: #2ecc71;
            color: #ffffff;
            font-weight: bold;
            padding: 10px 20px;
            font-size: 18px;
            border-radius: 5px;
            transition: background-color 0.3s ease;
        }
        div[data-testid="stButton"] button:hover {
            background-color: #27ae60;
        }
    </style>
""", unsafe_allow_html=True)
if st.button("Collect and Store data"):
    chan_ids = []
    db = client["Youtube_Details"]
    Coll1 = db["Channel_Details"]
    for chdata in Coll1.find({}, {"_id": 0, "Channel_Information": 1}):
        chan_ids.append(chdata["Channel_Information"]["Channel_Id"])

    if channel_id in chan_ids:
        st.success("Channels Details of the given Channel Id already exists")
    else:
        # Function to collect and store channel details
        insert = full_Channel_Details(channel_id)
        st.success(insert)

all_channels=[]
db = client["Youtube_Details"]
Coll1 = db["Channel_Details"]
for channel_data in Coll1.find({}, {"_id": 0, "Channel_Information": 1}):
    all_channels.append(channel_data["Channel_Information"]["Channel_Name"])

unique_chan = st.selectbox("Select the Channel",all_channels)

# Button to trigger migration of data to SQL tables
if st.button("Migrate to SQL"):
    # Function to create SQL tables
    tab = tables(unique_chan)
    st.success(tab)

# Radio button for selecting the table to view
show_table = st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

# Conditional display of tables based on user selection
if show_table == "CHANNELS":
    show_channels_table()

elif show_table == "PLAYLISTS":
    show_playlists_table()

elif show_table == "VIDEOS":
    show_vidoes_table()

elif show_table == "COMMENTS":
    show_comments_table()

#SQL Connection
mydb = mysql.connector.connect(host = "localhost",
                            user = "root",
                            password = "kiran",
                            database = "youtube_harvest_warehousing",
                            port ="3306" )
cursor = mydb.cursor()

# Streamlit part - SQL queries based on user-selected questions
st.markdown("""
    <style>
        .select-box-container {
            display: flex;
            justify-content: center;
        }
        .select-box-container select {
            background-color: #3498db;
            color: #ffffff;
            font-weight: bold;
            padding: 10px;
            font-size: 16px;
            border: none;
            border-radius: 5px;
            margin: 0 10px;
            cursor: pointer;
            transition: background-color 0.3s ease;
        }
        .select-box-container select:hover {
            background-color: #2980b9;
        }
    </style>
""", unsafe_allow_html=True)

question=st.selectbox("Select your Question",("1.All the videos and their corresponding channels",
                                              "2.Channels with the most number of videos",
                                              "3.Top 10 most viewed videos and their respective channels",
                                              "4.Count of comments were made on each video with their corresponding video names",
                                              "5.Videos have the highest number of likes with their corresponding channel names",
                                              "6.Total number of likes and dislikes for each video with their corresponding video names",
                                              "7.Total number of views for each channel with their corresponding channel names",
                                              "8.All the channels that have published videos in the year 2022",
                                              "9.Average duration of all videos in each channel with their corresponding channel names",
                                              "10.Videos have the highest number of comments with their corresponding channel names"
))


# Execute SQL queries based on the selected question
if question=="1.All the videos and their corresponding channels":
    # Query to retrieve videos and their corresponding channels
    query1= """select title as videos,channel_name as channelname from videos"""
    cursor.execute(query1)
    t1=cursor.fetchall()
    mydb.commit()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2.Channels with the most number of videos":
    # Query to retrieve Channels with the most number of videos
    query2= """select channel_name as channelname,total_videos as no_videos from channels order by total_videos desc"""
    cursor.execute(query2)
    t2=cursor.fetchall()
    mydb.commit()
    df2=pd.DataFrame(t2,columns=["Channel name","No of videos"])
    st.write(df2)

elif question=="3.Top 10 most viewed videos and their respective channels":

    #Query to retrieve Top 10 most viewed videos and their respective channels
    query3= """select views as views,channel_name as channelname,title as videotitle from videos
                where views is not null order by views desc limit 10"""
    cursor.execute(query3)
    t3=cursor.fetchall()
    mydb.commit()
    df3=pd.DataFrame(t3,columns=["Views","Channel_Name","Video_Title"])
    st.write(df3)

elif question=="4.Count of comments were made on each video with their corresponding video names":

    #Query to retrieve Count of comments were made on each video with their corresponding video names
    query4= """select comments as no_comments,title as videotitle from videos where comments is not null"""
    cursor.execute(query4)
    t4=cursor.fetchall()
    mydb.commit()
    df4=pd.DataFrame(t4,columns=["No of Comments","Video_Title"])
    st.write(df4)

elif question=="5.Videos have the highest number of likes with their corresponding channel names":

    #Query to retrieve Videos have the highest number of likes with their corresponding channel names
    query5= """select title as videotitle,channel_name as channelname, likes as likecount from videos 
                where likes is not null order by likes desc"""
    cursor.execute(query5)
    t5=cursor.fetchall()
    mydb.commit()
    df5=pd.DataFrame(t5,columns=["Video_Title","Channel_Name","Likes_Count"])
    st.write(df5)

elif question=="6.Total number of likes for each video with their corresponding video names":

    #Query to retrieve Total number of likes for each video with their corresponding video names
    query6= """select likes as likescount,title as videotitle from videos"""
    cursor.execute(query6)
    t6=cursor.fetchall()
    mydb.commit()
    df6=pd.DataFrame(t6,columns=["Likes_Count","Video_Title"])
    st.write(df6)

elif question=="7.Total number of views for each channel with their corresponding channel names":

    #Query to retrieve Total number of views for each channel with their corresponding channel names
    query7= """select channel_name as channelname,views as totalviews from channels"""
    cursor.execute(query7)
    t7=cursor.fetchall()
    mydb.commit()
    df7=pd.DataFrame(t7,columns=["Channel Name","Total No of views"])
    st.write(df7)

elif question=="8.All the channels that have published videos in the year 2022":

    #Query to retrieve All the channels that have published videos in the year 2022
    query8= """select title as video_title,published_date as videorelease,channel_name as channelname from videos 
                where extract(year from published_date)=2022"""
    cursor.execute(query8)
    t8=cursor.fetchall()
    mydb.commit()
    df8=pd.DataFrame(t8,columns=["Video Title","Published Date","Channel Name"])
    st.write(df8)

elif question=="9.Average duration of all videos in each channel with their corresponding channel names":

    #Query to retrieve Average duration of all videos in each channel
    query9= """select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name"""
    cursor.execute(query9)
    t9=cursor.fetchall()
    mydb.commit()
    df9=pd.DataFrame(t9,columns=["Channel Name","Average Duration"])
    
    T9 = []
    for index, row in df9.iterrows():
        channel_title = row["Channel Name"]
        average_duration_seconds = int(row["Average Duration"])  # Convert to int

        # Calculate days, hours, minutes, and seconds
        days = average_duration_seconds // (24 * 3600)
        remaining_seconds = average_duration_seconds % (24 * 3600)
        hours = remaining_seconds // 3600
        remaining_seconds %= 3600
        minutes = remaining_seconds // 60
        seconds = remaining_seconds % 60

        # Format the duration string without decimal values
        duration_str = f"{days} days {hours:02}:{minutes:02}:{seconds:02}"

        T9.append(dict(channeltitle=channel_title, avgduration=duration_str))

    df1 = pd.DataFrame(T9)
    st.write(df1)

elif question=="10.Videos have the highest number of comments with their corresponding channel names":

    #Query to retrieve Videos have the highest number of comments with their corresponding channel names
    query10= """select title as videotitle, channel_name as channelname, comments as comments from videos
                where comments is not null order by comments desc"""
    cursor.execute(query10)
    t10=cursor.fetchall()
    mydb.commit()
    df10=pd.DataFrame(t10,columns=["Video_Title","Channel_Name","Comments"])
    st.write(df10)