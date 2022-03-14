import pymysql.cursors


def create_top_albums_db():
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='Reiffman8',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sqltest = "DROP DATABASE IF EXISTS top_albums"
            cursor.execute(sqltest)
            sql = "CREATE DATABASE top_albums"  # will create a database called top_albums
            cursor.execute(sql)
            # Uses top_albums database
            sql_use = "USE top_albums"
            cursor.execute(sql_use)
            sql_create_table_albums = "CREATE TABLE albums (\
                                      album_id int PRIMARY KEY AUTO_INCREMENT,\
                                      album_name varchar(255),\
                                      album_link varchar(255),\
                                      artist_id int,\
                                      publisher_id int,\
                                      metascore int,\
                                      user_score float,\
                                      num_of_critic_reviews int,\
                                      num_of_user_reviews int,\
                                      release_date datetime,\
                                      summary_id int,\
                                      details_and_credits_link varchar(255),\
                                      amazon_link TEXT,\
                                      scrape_date datetime\
                                      )"
            sql_create_table_genres = "CREATE TABLE genres (\
                                      genre_id int PRIMARY KEY AUTO_INCREMENT,\
                                      genre_name varchar(255) UNIQUE\
                                      )"
            sql_create_table_albums_to_genres = "CREATE TABLE albums_to_genres (\
                                                album_id int,\
                                                genre_id int\
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
                                          summary TEXT\
                                          )"

            cursor.execute(sql_create_table_albums)
            cursor.execute(sql_create_table_genres)
            cursor.execute(sql_create_table_albums_to_genres)
            cursor.execute(sql_create_table_artists)
            cursor.execute(sql_create_table_publishers)
            cursor.execute(sql_create_table_summaries)

            cursor.execute("COMMIT")

def connect_to_db():
    """
  function that connects to top_albums database
  """

    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='Reiffman8',
                                 database='top_albums',
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection.cursor()
