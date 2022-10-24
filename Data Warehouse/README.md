This project covers a use case for **Sparkify** startup. By data modeling with Postgres and building an ETL pipeline using Python, to create a database that maintans data of logs on user activity on the app and the songs in their app to optimize queries on song play analysis. 


## Schema design and ETL pipeline

> ETL pipeline is strucutred on AWS. Data is extracted from two s3 buckets, one for the log data and one for song info data. The raw data is then stored into two corresponding staging tables in redshift with schema for analytics tables in mind. Staging tables are then transformed  into  five analytics tables  stored in redshift.
For the five tables, Schema follows a star schema with a fact table (songplays) for records in log data associated with song plays and Dimension tables for users of the app (users table), songs in library (songs), artists in library (artists) and timestamps of records in songplays broken down into specific units (time)

## How to run 
> python create_tables.py    # runs script for creating the tables

> python etl.py              # runs script for the etl pipeline


## files in repo
>  create_tables.py :  creates tables in Redshift

>  etl.py : runs the etl pipeline. loads data from S3 into staging tables on Redshift and then process that data into  analytics tables on Redshift

>  sql_queries.py :   contains SQL queries for drop, create, copy and insertion of tables which will be imported into the two other files above

> dwh.cfg  : configuration file for aws credentials, cluster info. and S3 buckets paths

