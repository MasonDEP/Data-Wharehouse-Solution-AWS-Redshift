# Data Wharehouse Solution with AWS Redshift

Tech Stack: AWS IAC, Redshift, S3, Python, SQL

This project is a cloud data wharehouse solution for a popular music streaming app to store, analyze and gather insight from its user activity data.The project aims to understand what songs users are listening to. The company has user activity data in JSON format stored in an AWS S3 bucket and want to build a reliable data wharehouse to run multiple ETL processes in parallel and achieve data engineering at scale.

In this project, as the lead data engineer, I first built a python script to programmatically create the AWS infrastructure I need. Practicing the 'Infrastructure as Code' principle, I created an IAM role, an Redshift Cluster and configured the TCP port for connection to the notebook. As a second step, I created a set of DDL and 'INSERT' statement to first create the tables that I need then load data into them.

As to the schema design, 4 fact tables that contained detailed inforamtion on users, songs, artist and time of play are put in place to reduce data replication. Alongside a fact table called 'songplays', the 5 tables make up a star schema design to simplify queries and improve aggregation speed. 

## Data Process Flow

>[RAW ZONE] --> [STAGING ZONE] --> [CURATION ZONE]

- RAW ZONE: The source data are a number of json files residing in a set of s3 directories.
- STAGING ZONE: The data is copied to the 'events_staging' and 'songs_staging' tables without any transformation.
- CURATION ZONE: The data is transformed and loaded into the 4 dimention tables and 1 fact table in the curation zone.


## Source Data

The source Data for this project are two sets of JSON files that contains information realted to the songs and user play history respectively.

#### Song JSON files
Song files are partitioned by the first three letters of each song's track ID and are a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/).  The following filepath and its content are given as an example:

> song_data/A/A/B/TRAABJL12903CDCF1A.json

> {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

#### Log JSON files

Log files are partitioned by year and month as follows and is generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate app activity logs from a music streaming app based on specified configurations.:

> log_data/2018/11/2018-11-13-events.json


## Table Schema

#### Fact Table

| songplays |
| --- |
| songplay_id |
| start_time |
| user_id |
| level |
| song_id |
| artist_id |
| session_id |
| location |
| user_agent |
> Records in log data associated with song plays i.e. records with page

#### Dimension Tables

| users  |
| --- |
| user_id |
| first_name |
| last_name |
| gender |
| level |
> App users 

| songs   |
| --- |
| song_id |
| title |
| artist_id |
| year |
| duration |
> Songs in music database

| artists    |
| --- |
| artist_id |
| name |
| location |
| lattitude |
| longitude |
> Artists in music database

| time     |
| --- |
| start_time |
| hour |
| day |
| week |
| month |
| year |
| weekday |

## File descriptions

`sql_queries.py` contains SQL queries that are execued by other files.

`create_tables.py` drops and reinitialises the database environment using queries from `sql_queries.py`.

`etl.py` stores the DDL, data transformation and table insert queries in SQL.

`dwh_temp` contains a set of AWS infrastruction related key information that needs to be filled before run.
