import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events ( 
artist text,
auth text,
firstName text,
gender text,
itenInSession int,
lastName text,
length float8,
level text,
location text,
method text,
page text,
registration text,
sessionId int,
song text,
status int,
ts text,
userAgent text,
userId int);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
artist_id text,
artist_latitude float8,
artist_location text, 
artist_longitude float8,
artist_name text,
duration float8,
num_songs int,
song_id text,
title text,
year int);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id bigint identity(0, 1),
start_time timestamp not null, 
user_id int not null, 
level text, 
song_id text not null, 
artist_id text, 
session_id int, 
location text, 
user_agent text);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int, 
first_name text, 
last_name text,
gender text,
level text);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id text, 
title text, 
artist_id text not null, 
year int, 
duration float8);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id text, 
name text, 
location text,
latitude float8, 
longitude float8);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP, 
hour int, 
day int,
week int, 
month int, 
year int,
weekday int);
""")

# STAGING TABLES

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json 'auto';
""".format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN']))


staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json {};
""".format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH']))


# FINAL TABLES

songplay_table_insert = ("""
insert into songplays (artist_id, level, location, session_id, song_id, start_time, user_agent, user_id) (
select b.artist_id, level, location, sessionid, song_id, 
timestamp 'epoch' + CAST(ts AS BIGINT)/1000  * interval '1 second' as start_time, 
useragent, userid
from staging_events a join staging_songs b on a.artist = b.artist_name 
and a.song = b.title
where page = 'NextSong');
""")

user_table_insert = ("""
insert into users(first_name, gender, last_name, level, user_id) (
select firstname, gender, lastname, level, userid
from staging_events
group by firstname, gender, lastname, level, userid
);
""")

song_table_insert = ("""
insert into songs(artist_id, duration, song_id, title, year) (
select artist_id, duration, song_id, title, year
from staging_songs
group by artist_id, duration, song_id, title, year
);
""")

artist_table_insert = ("""
insert into artists(artist_id, latitude, location, longitude, name) (
select artist_id, artist_latitude, artist_location, artist_longitude, artist_name
from staging_songs
group by artist_id, artist_latitude, artist_location, artist_longitude, artist_name
);
""")

time_table_insert = ("""
insert into time(start_time, day, hour, month, week, weekday, year) (
select start_time, 
  extract(day from start_time), extract(hour from start_time), extract(month from start_time),
  extract(week from start_time), extract(weekday from start_time),
  extract(year from start_time)
from  (select start_time from songplays group by start_time) d
);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
