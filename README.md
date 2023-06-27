# YouTube-Data-Harvesting-Warehousing



### In This DataSyncpro Project we would get YouTube Channel data from YouTube Api with the help of 'Channel ID' , We Will Store the channel data into Mongo DB Atlas as a Document then the data Would convert into Sql Records for Data Analysis. This Entire Project depends on Extract Transform Load Process(ETL).

## Explanation To Do Execute The  Project : 

      Step 1: Install/Import Modules Streamlit , pandas , json , pymongo ,  psycopg2  ,googleapiclient . isodate

      Step 2: I have created 11 different methods inside the class "YT2SQL" 
                  
                   Process of Methods :

                               Method 1 :  This  method would get YouTube Channel API  data  with the help of channel id , Would create the channel details in json format data.

                               Method 2 :  This method will get the channel "playlist id" as a input then returns all the video ids of the provided playlist id .

                               Method 3 :  This method will get the channel "Video ids" as a input then returns all the video and comment details  of provided video ids of given channel  .

                               Method 4:   This method  will return the playlist id , video id and channel id of corresponding channels in json format
                               
                               Method 5 :  Merging All Details Of Given Channel Together that consist channel details , video details , playlist details then return as single channel json data format

                               Method 6 : It will inert channel data into mongo DB Atlas as a document
 
                               Method 7 :  It will get  Channel Names From MongoDB Documents for verification purpose

                               Method 8 : This method will fetch   Mongo Document then convert into Dataframe for sql data insertion
                               
                               Method 9 :  In This method i did all data Transform process to  Load  data into sql

                               Method 10 : It will do Data Load Proces to sql 
                               
                               Method 11 : Data analaysis process using sql quries and python integreation

                             
                               
     Step 3 :  Run  Command prompt  with file location path that where you have the  "YtAPiproject.py" file ,
               In CMD do type this command  "streamlit run YtAPiproject.py"  . It will open on web browers that what you have eg: Google Chrome

     Step 4 : Make sure to connect your mongodb atlas and PostgresSQL DBMS  in your local setup .



## Skills Covered ✅ ⬇️

              Python (Scripting)
              
              Data Collection
              
              MongoDB
              
              SQL
              
              API Integration
              
              Data Managment using MongoDB (Atlas) and PostgresSQl
              
              IDE: Pycharm Community Version

           

                               

                                

                                

       







