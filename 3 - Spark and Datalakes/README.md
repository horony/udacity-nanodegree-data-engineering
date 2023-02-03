# Project: A Data Lake for Sparkify

In this project a data lake containing songplay-data from Sparkify and a corresponding ETL is constructed using EMR, S3 and Spark.

## Purpose
Sparkify is an upcoming audio streaming provider. In order to streamline product usability and drive revenue it is necessary for Sparkify to analyze user data. Structures enabling such analytical work of Data Analysts and Data Scientists must be created. Sparkify is still evolving quickly and changes to purposes of data and data structure happen often. Therefore a flexible data lake seems like good solution to enable Sparkifys short-term and long-term analytical goals.

## Schema and ELT Pipeline
As Data Analysts and Data Scientists need to answer a number of different questions a flexible star schema seems has been chosen to model the data. It allows users to combine the provided tables in convenient, fast and flexible fashion.

The star schema consists auf 1 fact table and 4 dimension tables:
- **songplays** (fact, all songs played by users on Sparkify)
- **users** (dimension, information about  all users that played a song on Sparkify)
- **songs** (dimension, information about specific songs)
- **artists** (dimension, information about specific artists)
- **time** (dimension, timestamps of all playes songs with additional information)

As Sparkify deals with a big amount of data which is only expected to grow a cloud datalake based on AWS is a good solution for the ETL pipeline. S3 Storage, EMR Nodes and Spark parallelization provided by AWS enables Sparkifies analytical deparment flexible and scalable options. Thus fits the needs of a evolving startup.

## Files and How-To
In order to run this file you need an running *EMR cluster* with a connected *S3 bucket* on *AWS*.
If both needs are satisfied your AWS credentials should be placed in the *dl.cg*
Afterwards you can run the *etl.py* script. This starts the ETL by using EMR and Spark to read from S3, wrangle the data and write to S3.