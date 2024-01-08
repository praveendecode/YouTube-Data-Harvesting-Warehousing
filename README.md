# YouTube Data Harvesting Warehousing and Data Analysis
![image](https://github.com/praveendecode/YouTube-Data-Harvesting-Warehousing/assets/95226524/30d29168-225f-48bb-920e-b16570054169)



# Overview

 - This project is focused on harvesting data from YouTube channels using the YouTube API, processing the data, and warehousing it. The harvested data is initially stored in a MongoDB Atlas database as documents and is then converted into SQL records for in-depth data analysis. The project's core functionality relies on the Extract, Transform, Load (ETL) process.
      Features

# Approach 

  - Harvest YouTube channel data using the YouTube API by providing a 'Channel ID'.
    
  - Store channel data in MongoDB Atlas as documents
    
  - Convert MongoDB data into SQL records for data analysis.
    
  - Use 11 different methods within the "YT2SQL" class to perform various tasks.
    
  - Implement Streamlit to present code and data in a user-friendly UI.
    
  - Execute data analysis using SQL queries and Python integration.

# Getting Started

  - Install/Import the necessary modules: Streamlit, Pandas, PyMongo, Psycopg2, Googleapiclient, and Isodate.
    
  - Ensure you have access to MongoDB Atlas and set up a PostgresSQL DBMS on your local environment.

# Technical Steps to Execute the Project

### Step 1: Install/Import Modules

   - Ensure the required Python modules are installed: Streamlit, Pandas, PyMongo, Psycopg2, Googleapiclient, and Isodate.

### Step 2: Utilize the "YT2SQL" Class

   - There are 11 methods within the class, each with specific functionality for data extraction and transformation. These methods cover tasks like data retrieval, data storage, and data analysis.

### Step 3: Run the Project with Streamlit

   - Open the command prompt in the directory where "YtAPiproject.py" is located.
   - Execute the command: streamlit run YtAPiproject.py. This will open a web browser, such as Google Chrome, displaying the project's user interface.

### Step 4: Configure Databases

   - Ensure that you are connected to both MongoDB Atlas and your local PostgresSQL DBMS.

# Methods

   - Get YouTube Channel Data: Fetches YouTube channel data using a Channel ID and creates channel details in JSON format.
     
   - Get Playlist Videos: Retrieves all video IDs for a provided playlist ID.
     
   - Get Video and Comment Details: Returns video and comment details for the given video IDs.
     
   - Get All Channel Details: Provides channel, video, and playlist details in JSON format.
     
   - Merge Channel Data: Combines channel details, video details, and playlist details into a single JSON format.
     
   - Insert Data into MongoDB: Inserts channel data into MongoDB Atlas as a document.
     
   - Get Channel Names from MongoDB: Retrieves channel names from MongoDB documents.
     
   - Convert MongoDB Document to Dataframe: Fetches MongoDB documents and converts them into dataframes for SQL data insertion.
     
   - Data Transformation for SQL: Performs data transformation for loading into SQL.
     
   - Data Load to SQL: Loads data into SQL.
     
   - Data Analysis: Conducts data analysis using SQL queries and Python integration.
     
   - Manage MongoDB Documents: Manages MongoDB documents with various options.
     
   - Delete SQL Records: Deletes SQL records related to the provided YouTube channel data with various options.

# Tools Expertise 

   - Python (Scripting)
     
   - Data Collection
     
   - MongoDB
     
   - SQL
     
   - API Integration
     
   - Data Management using MongoDB (Atlas) and PostgreSQL
     
   - IDE: PyCharm Community Version

# Result :

   - This project focuses on harvesting YouTube data using the YouTube API, storing it in MongoDB, converting to SQL for analysis. Utilizes Streamlit, Python, and various methods for ETL. Expertise includes Python, MongoDB, SQL, API integration, and data management tools . This project maily reduces 80% percentage of manually data processing and data storing work effectively.
