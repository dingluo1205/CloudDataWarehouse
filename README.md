# CloudDataWarehouse

## Objective
To build an ETL pipeline for a database hosted on Redshift. 


## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, I load the data from S3 to staging tables on Redshift, and execute the SQL statement to create analytical tables
based on star schema from staging tables. 

## Files 
### create_tables.py 
It will create fact and dimension tables for the star schema in redshift 

### dwh.cfg 
It stores information about config, and connections to the redshift. Due to account security consideration, I've deleted the data in the config file. 

### etl.py
It loads the data from S3 to staging tables on Redshift, and executes the SQL statement for processing data 

### sql_queries.py
It defines the sql statements, and are imported to the other two py files 

## Tables
There are three types of tables here in this project. 
1. For staging tables 
I created two staging tables (staging_events and staging_songs) to store data from two S3 buckets. 
2. For fact tables 
As I used star schema for designing my database, the fact table I created is for song play event. It stores data about artist_id, level, location, session_id, song_id, start_time, user_agent, user_id. 
3. For dimension tables 
I created four dimension tables - users, artists, songs, and time. 

## How to run the scripts to make the ETL happen? 
1. Run the create_tables.py to create all the tables needed in this database 
2. Run the etl.py to finish the ETL process including loading data to staging tables, and inserting data to dimension/fact tables 
