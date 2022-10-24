# Data Modeling with Postgres


## Description

> This project covers a use case for **Sparkify** startup. By data modeling with Postgres and building an ETL pipeline using Python, to create a database that maintans data of logs on user activity on the app and the songs in their app to optimize queries on song play analysis. 


## Files

> 
    test.ipynb displays the first few rows of each table to let you check your database.
    create_tables.py drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
    etl.ipynb reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL       process for each of the tables.
    etl.py reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
    sql_queries.py contains all your sql queries, and is imported into the last three files above.
    README.md provides discussion on your project.


## Schema design and ETL pipeline

> Schema follows a star schema with a fact table (songplays) for records in log data associated with song plays and Dimension tables for users of the app (users table), songs in library (songs), artists in library (artists) and timestamps of records in songplays broken down into specific units (time)
Fact Table
    **songplays** 
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agen
Dimension Tables
    **users** 
        user_id, first_name, last_name, gender, level
    **songs** 
        song_id, title, artist_id, year, duration
    **artists** 
        artist_id, name, location, latitude, longitude
    **time**
        start_time, hour, day, week, month, year, weekday
        

## Run 

Run pyhton script create_tables.py then etl.py 

command: 
    $ python create_tables.py
    $ python etl.py
