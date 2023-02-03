# Cloud Data Warehouse for Sparkify

This project creates an analytical data warehouse for the music steaming provider *Sparkify*. The project uses *Python* to read song-data and log-data from *S3*, transform it and write it into a *Redshift*-database.

---

## 1. Purpose

The startup *Sparkify* is a fast growing music streaming provider. In order to streamline the product in terms of usability and revenue *Sparkify* needs their Data Science-departments to analyze user data. As *Sparkify* is evolving quickly and data is only expected to grow a cloud data warehouse seems like a good solution to address *Sparkifys* needs. Its flexibilty allows *Sparkfiy* to easily adjust the scale, size and cost of their new analytical database.

---

## 2. Database schema design 

The databse utilizes a star schema to enable flexible usage of the provided user data.

The raw data is stored in to staging tables:
- stage_log_data
- stage_song_data

The staging data is then transformed within the ETL into 1 fact table and 4 dimension tables:
- songplays (fact, all songplay-actions logged by *Sparkify*)
- users (dimension, information about unique *Sparkify*-users)
- time (dimension, meta-data such as year unique timestamps of songplays)
- songs (dimension, information about unique songs)
- artists (dimension, information about unique artists)

---

## 3. ETL pipeline

The ETL pipeline utilizes ***Python***, ***S3*** and ***Redshift***. The ETL starts by setting up the database empty table elements in *Redshift*: 2 staging tables, 1 production fact table and 4 production dimension tables. Log-data and song-data is provided in two *S3*-Buckets. The computing power of our new *Redshift* is used to copy the raw data from *S3* into the *Redshift*-staging tables. Afterwards again in-database-transformation are used to transform the staging data into the 5 production tables.

---

## 4. How to 

If you want to excute the script follow the following steps:

1. In order to run this scripts you first need to set up *Redshift*-database (see https://aws.amazon.com/redshift/). Afterwars copy your credentials into the ***dwh.cfg*** file.
2. Excute the ***create_tables.py***-script with *Python*. It will connect to your database and use SQL-queries from ***sql_queries.py*** to set up the table structure in your database.
3. Excute the ***etl.py***-script with *Python*. It will connect to your database and use SQL-queries from ***sql_queries.py*** to load data from *S3* into your staging tables and transform the staging data into the 5 production tables.

