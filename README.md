# Datawarehouse_Project-viaudacity-

This project was aimed to build an ETL pipeline for a streaming music startup. In this manner, data in S3 was extracted in a Redshift cluster and transformed into dimensional tables in order to provide ease to make data analytics.


## Project Roadmap and Files Repository

At the beginning of the project, the sql_queries.py file was generated via SQL in order to specify queries and adjust the tables. Designing tables was organized by distribution and sorting strategy in order to avoid 'shuffle' concept. Also, 'dwh.cfg' file was edited in order to connect redshift cluster via IAM role. Then, 'create_tables.py' and 'etl.py' were tested in 'testrunning.ipynb' in order to check condition of the data in Redshift. 


## Data Warehouse Design

In order to implement given data to build ETL pipeline, A fact table and four dimension tables were builded. These are shown in the figure below.


### FACT TABLE

|songplays  |
| ------------- | 
| songplay_id (primary key) | 
| start_time (sort key) | 
| user_id     (distributed key)  | 
| level  | 
| song_id  | 
| artist_id  | 
| session_id  | 
| location  | 

### DIMENSION TABLES

|users  |songs  |
| ------------- | ------------- |
| user_id (primary key) | song_id   (primary and sortkey key) |
| first_name|title| 
| last_name | artist_id (distrubuted key) |
| gender (distributed key) | duration |
| song_id  | 
| level  | 
|time|artists   |
| ------------- | ------------- |
| start_time   (primary and sortkey key)| artist_id (primary key and sort key) |
|hour| name (distrubuted key) | 
| day | location |
| week | latitude |
| month | longitude  |
| year (distrubuted key) |             
| weekday |             




### artists                           
"artist_id (primary and sort key)       
"name      (distributed key)          
"location                             
"latitude                             
"longitude                                                     
                                      
### time
"start_time (primary and sort key)
"hour
"day
"week
"month
"year       (distrubuted key)
"weekday

## IMPLEMENTATION OF MODELLING

1- In sql_queries.py, SQL query statements were edited for create_tables.py and etl.py. This file consists queries in order to creat, drop designed tables and insert table from data where located in S3 bucket.
2- In create_tables.py, functions defined in order to create, drop related tables. Also, sparkify database in S3 was created and connected by functions in the file. In conlusion, returns Redshift cluster cursor. Finally, it Redshift cluster cursor.
3- In etl.py, inserts data from S3 bucket throught tables. Also, it reads and organizes the data from base folders.
