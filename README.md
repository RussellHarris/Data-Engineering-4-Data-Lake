# Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

# Project Description
In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

# Files
* etl.py - RUN THIS - Ingests data from Udacity S3 source, then saves to your S3 instance
* etl.ipnb - Used for testing of code prior to copying to etl.py
* dl.cfg - Put your AWS credentials here
* data.zip - Sample files to use on local disk prior to running against files on S3

# Instructions
1. **Update** *dl.cfg* with your AWS credentials
2. **Run** 'python etl.py'

# Schema Design
* **songplays** - fact table derived from log_data+song_data
* **users** - dimension table, unique user records with "user level" by most recent timestamp from log_data
* **songs** - dimension table, unique song records from song_data
* **artists** - dimension table, artists records from song_data
* **time** - dimension table, unique time records from log_data
