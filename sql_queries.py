import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_songs_table_create= ("""CREATE TABLE IF NOT EXISTS songs_staging
                                (
                                    num_songs    integer not null,
                                    artist_id    varchar not null,
                                    artist_latitude    varchar,
                                    artist_longitude   varchar,
                                    artist_location   varchar,
                                    artist_name    varchar not null,
                                    song_id    varchar not null,
                                    title    varchar not null,
                                    duration    numeric not null,
                                    year    integer not null   sortkey distkey
                                )"""
                            )

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS events_staging
                                  (
                                    artist    varchar,
                                    auth    varchar not null,
                                    firstName    varchar,
                                    gender    varchar(10),
                                    itemInSession    integer not null,
                                    lastName    varchar,
                                    length    numeric,
                                    level    varchar not null,
                                    location    varchar,
                                    method    varchar not null,
                                    page    varchar not null,
                                    registration    numeric,
                                    sessionId    integer not null,
                                    song    varchar,
                                    status    integer not null,
                                    ts    numeric not null,
                                    userAgent    varchar,
                                    userId    integer sortkey distkey
                                   )"""
                              )


songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays 
                            (
                                songplay_id integer identity(0, 1) not null sortkey, 
                                start_time timestamp not null, 
                                user_id integer not null distkey, 
                                level varchar, 
                                song_id varchar, 
                                artist_id varchar, 
                                session_id integer not null, 
                                location varchar, 
                                user_agent varchar
                            );"""
                        )

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
                        (
                            user_id integer NOT NULL sortkey, 
                            first_name varchar NOT NULL, 
                            last_name varchar NOT NULL, 
                            gender varchar NOT NULL, 
                            level varchar NOT NULL
                        );"""
                    )

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                        (
                            song_id varchar NOT NULL sortkey,
                            title varchar NOT NULL,
                            artist_id varchar NOT NULL,
                            year int,
                            duration decimal
                        );"""
                    )

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                            (
                                artist_id varchar NOT NULL sortkey, 
                                name varchar NOT NULL, 
                                location varchar, 
                                latitude decimal, 
                                longitude decimal
                            );"""
                      )

time_table_create = ("""CREATE TABLE IF NOT EXISTS times
                        (
                            start_time timestamp NOT NULL sortkey, 
                            hour integer NOT NULL, 
                            day integer NOT NULL, 
                            week integer NOT NULL, 
                            month integer NOT NULL, 
                            year integer NOT NULL, 
                            weekday integer NOT NULL
                        );"""
                    )

# STAGING TABLES

staging_events_copy = ("""
                            copy events_staging from {}
                            iam_role '{}'
                            format as json {}
                        """).format(
                            config.get("S3", "LOG_DATA"),
                            config.get("IAM_ROLE","ARN"),
                            config.get("S3", "LOG_JSONPATH")
)
staging_songs_copy = ("""
                        copy songs_staging from {}
                        iam_role '{}'
                        json 'auto'
                    """).format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES
songplay_table_insert = ("""
    insert into songplays (
        start_time, user_id, level, song_id, artist_id,
        session_id, location, user_agent
    )
    select
        timestamp 'epoch' + e.ts / 1000 * interval '1 second' as start_time,
        e.userId as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId as session_id,
        e.location,
        e.userAgent as user_agent
    from events_staging e
    left join songs_staging s on e.song = s.title and e.artist = s.artist_name
    where e.page = 'NextSong'
""")

user_table_insert = ("""
    insert into users
    select e.userId as user_id, e.firstName as first_name, e.lastName as last_name, e.gender, e.level
    from events_staging e
    join (
        select max(ts) as ts, userId
        from events_staging
        where page = 'NextSong'
        group by userId
    ) ns on e.userId = ns.userId and e.ts = ns.ts
""")

song_table_insert = ("""
    insert into songs
    select
        song_id,
        title,
        artist_id,
        year,
        duration
    from songs_staging
""")

artist_table_insert = ("""
    insert into artists
    select distinct
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    from songs_staging
""")

time_table_insert = ("""
    insert into times
    select
        time.start_time,
        extract(hour from time.start_time) as hour,
        extract(day from time.start_time) as day,
        extract(week from time.start_time) as week,
        extract(month from time.start_time) as month,
        extract(year from time.start_time) as year,
        extract(weekday from time.start_time) as weekday
    from (
        select distinct
            timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time
        from events_staging
        where page = 'NextSong'
    ) time
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
