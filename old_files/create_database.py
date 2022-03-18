import pymysql.cursors


def create_top_albums_db(login_info):
    connection = pymysql.connect(host='localhost',
                                 user=login_info['username'],
                                 password=login_info['password'],
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql_drop = "DROP DATABASE IF EXISTS top_albums"
            cursor.execute(sql_drop)
            sql = "CREATE DATABASE top_albums"  # will create a database called top_albums
            cursor.execute(sql)
            # Uses top_albums database
            sql_use = "USE top_albums"
            cursor.execute(sql_use)
            sql_create_table_history = "CREATE TABLE chart_history (\
                                      scrape_id int PRIMARY KEY AUTO_INCREMENT,  \
                                      scrape_datetime datetime,\
                                      chart_id int,\
                                      album_id int,\
                                      album_rank int,\
                                      artist_id int,\
                                      publisher_id int,\
                                      metascore int,\
                                      user_score float,\
                                      num_of_critic_reviews int,\
                                      num_of_user_reviews int,\
                                      summary_id int\
                                      )"
            sql_create_table_charts = "CREATE TABLE charts (\
                                      chart_id int PRIMARY KEY AUTO_INCREMENT, \
                                      filter_by varchar(255), \
                                      year int, \
                                      sort_by varchar(255)\
                                      )"
            sql_create_table_albums = "CREATE TABLE albums (\
                                      album_id int PRIMARY KEY AUTO_INCREMENT, \
                                      album_name varchar(255) UNIQUE, \
                                      album_link varchar(255), \
                                      details_and_credits_link varchar(255),\
                                      amazon_link TEXT,\
                                      release_date datetime\
                                      )"
            sql_create_table_genres = "CREATE TABLE genres (\
                                      genre_id int PRIMARY KEY AUTO_INCREMENT,\
                                      genre_name varchar(255) UNIQUE\
                                      )"
            sql_create_table_albums_to_genres = "CREATE TABLE albums_to_genres (\
                                                album_id int,\
                                                genre_id int,\
                                                PRIMARY KEY (album_id, genre_id)\
                                                )"
            sql_create_table_artists = "CREATE TABLE artists (\
                                        artist_id int PRIMARY KEY AUTO_INCREMENT,\
                                        artist_name varchar(255) UNIQUE,\
                                        artist_link varchar(255))"
            sql_create_table_publishers = "CREATE TABLE publishers (\
                                          publisher_id int PRIMARY KEY AUTO_INCREMENT,\
                                          publisher_name varchar(255) UNIQUE,\
                                          publisher_link varchar(255)\
                                          )"
            sql_create_table_summaries = "CREATE TABLE summaries (\
                                          summary_id int PRIMARY KEY AUTO_INCREMENT,\
                                          summary VARCHAR(767) UNIQUE\
                                          )"

            cursor.execute(sql_create_table_history)
            cursor.execute(sql_create_table_charts)
            cursor.execute(sql_create_table_albums)
            cursor.execute(sql_create_table_genres)
            cursor.execute(sql_create_table_albums_to_genres)
            cursor.execute(sql_create_table_artists)
            cursor.execute(sql_create_table_publishers)
            cursor.execute(sql_create_table_summaries)

            cursor.execute("COMMIT")

            foreign_key1 = "ALTER TABLE albums_to_genres\
                                    ADD FOREIGN KEY (album_id)\
                                    REFERENCES albums (album_id)"
            foreign_key2 = "ALTER TABLE albums_to_genres\
                                    ADD FOREIGN KEY (genre_id)\
                                    REFERENCES genres (genre_id)"
            foreign_key3 = "ALTER TABLE chart_history\
                                    ADD FOREIGN KEY (album_id)\
                                    REFERENCES albums (album_id)"
            foreign_key4 = "ALTER TABLE chart_history\
                                    ADD FOREIGN KEY (artist_id)\
                                    REFERENCES artists (artist_id)"
            foreign_key5 = "ALTER TABLE chart_history\
                                    ADD FOREIGN KEY (publisher_id)\
                                    REFERENCES publishers (publisher_id)"
            foreign_key6 = "ALTER TABLE chart_history\
                                    ADD FOREIGN KEY (summary_id)\
                                    REFERENCES summaries (summary_id)"
            foreign_key7 = "ALTER TABLE chart_history\
                                    ADD FOREIGN KEY (chart_id)\
                                    REFERENCES charts (chart_id)"

            cursor.execute(foreign_key1)
            cursor.execute(foreign_key2)
            cursor.execute(foreign_key3)
            cursor.execute(foreign_key4)
            cursor.execute(foreign_key5)
            cursor.execute(foreign_key6)
            cursor.execute(foreign_key7)
            cursor.execute("COMMIT")


# create_top_albums_db()