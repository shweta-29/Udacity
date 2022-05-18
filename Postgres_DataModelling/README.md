**Sparkify's Songs Database**

Sparkify has abeen collecting data on user activity and their songs library. The startup wants to create some insights out of this dataset. To provide data for analytics, I have created a data model for the team. This team consists of 1 fact table and 4 dimension tables.

![Star Schema](image.png)

Fact Table
1. songplays - records in log data associated with song plays 
• songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
2. users - users in the app
• user_id, first_name, last_name, gender, level
3. songs - songs in music database
• song_id, title, artist_id, year, duration
4. artists - artists in music database
• artist_id, name, location, latitude, longitude
5. time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

**How to run the Python scripts**
1. In a kernel, run the command : python create_tables.py
2. This will create all the tables. To insert data in the tables, run etl.py
3. To verify the tables content, use test.ipynb file

**An explanation of the files in the repository**
create_tables.py : This python script will create all the tables
data : contains song and log files dataset
etl.ipynb : To test the code for accessing data from json files and inserting data in the table
etl.py : This python script will insert data in all the tables
sql_queries.py : This python script contains SQL scripts for creating & dropping tables and inserting data in tables
test.ipynb : This notebook can be used to verify if the tables are created correctly.

**Database schema design and ETL pipeline.**
The schema design used is Star schema. This schema is used for denormalised tables. Hence, the ETL pipeline thus constructed will have a good performance when querying data. 
