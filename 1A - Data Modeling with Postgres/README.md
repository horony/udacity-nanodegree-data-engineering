# Summary

This script creates the **database _sparkifydb_** for the **music streaming company _Sparkify_**. 

## Database desgin

To allow data analysts and data scientists of Sparkify to convieniently analyize user behaviour the database was designed as relational postgres-database utilizing a star schema.

The star schema consist of:
1. fact table: _sparkifydb.songplays_
2. dimenstion table: _sparkifydb.artists_
3. dimenstion table: _sparkifydb.songs_ 
4. dimenstion table: _sparkifydb.users_
5. dimenstion table: _sparkifydb.time_
    
## ETL desgin

The ETL creates and inserts into the database tables in the following order to avoid foreign key conflict:

1. _sparkifydb.artists_
2. _sparkifydb.songs_ (references sparkifydb.artists.artist_id)
3. _sparkifydb.users_ 
4. _sparkifydb.time_
5. _sparkifydb.songplays_ (references all dimension tables)

The ETL interates trough .json-Data from Sparkigy log-files as well as song-files.

Duplicates in the primary keys four dimension tables are handled in the INSERT statements of the ETL in order to keep the dimension tables cleen.


# How to run

Open your terminal and call the following scripts to setup the database and complete a initial load.

1. Setup the database, intial table structures, relations and insert statements for the ETL by executing:
python create_tables.py

2. Execute a initial load for the database by parsing Song-Data and log-Data from Sparkify by executing:
python etl.py

3. Test success of the setup and initial load via test.ipynb.

# Files in the repository

1. README.md: Provides input on the project.
2. sql_queries.py: Contains all DROP, CREATE and INSERT SQL queries.
3. create_tables.py: Creates database and drops and creates tables by calling sql_queries.py.
4. etl.py: Reads and parses .json-Data into four dimension tables and one fact table and inserts them into database by calling sql_queries.py.
5. test.ipynb: Allows to test for success of create_tables.py and etl.py
6. etl.ipynb: Development environment for the project.

