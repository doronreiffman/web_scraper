"""
File that contains functions related to creating, connecting to, and adding scraped data to a database
Authors: Yair Vagshal and Doron Reiffman
"""
import config as cfg
import pymysql.cursors
import logging

# Logging definition
if cfg.LOGFILE_DEBUG:
    logging.basicConfig(filename=cfg.LOGFILE_NAME, format="%(asctime)s %(levelname)s: %(message)s",
                        level=logging.DEBUG)
else:
    logging.basicConfig(filename=cfg.LOGFILE_NAME, format="%(asctime)s %(levelname)s: %(message)s",
                        level=logging.INFO)


def connect_to_db(login_info, database=''):
    """
    Connect to created database using login info given (username and password).
    Returns the connection for use
    :param login_info: a dictionary with the username and password information
    :param database: a string with the name of the database
    """

    if database == '':
        connect = pymysql.connect(host='localhost',
                                  user=login_info['username'],
                                  password=login_info['password'],
                                  cursorclass=pymysql.cursors.DictCursor)
    else:
        connect = pymysql.connect(host='localhost',
                                  user=login_info['username'],
                                  password=login_info['password'],
                                  database=database,
                                  cursorclass=pymysql.cursors.DictCursor)
    return connect.cursor()


def create_top_albums_db(login_info):
    """
    Creates a database called 'top_albums' using login info given as a parameter.
    top_albums will hold the tables required to store data scraped from the album charts on the
    website Metacritic.
    See the README file for information on the tables and their columns
    :param login_info: a dictionary with the username and password information
    """

    with connect_to_db(login_info) as cursor:

        # Delete top_albums database if it exists, and then create top_albums
        sql_drop = "DROP DATABASE IF EXISTS top_albums"
        cursor.execute(sql_drop)
        sql = "CREATE DATABASE top_albums"
        cursor.execute(sql)

        # Uses top_albums database
        sql_use = "USE top_albums"
        cursor.execute(sql_use)

        sql_create_tables = dict()

        # Create charts table - contains information about charts (defined by the filter method, the year, and the
        # sort method
        sql_create_tables.update({"charts": "CREATE TABLE IF NOT EXISTS charts (\
                                  chart_id int PRIMARY KEY AUTO_INCREMENT, \
                                  filter_by varchar(255), \
                                  year int, \
                                  sort_by varchar(255)\
                                  );"})

        # Creates genres table - contains information about genres
        sql_create_tables.update({"genres": "CREATE TABLE IF NOT EXISTS genres (\
                                  genre_id int PRIMARY KEY AUTO_INCREMENT,\
                                  genre_name varchar(255) UNIQUE\
                                  );"})

        # Creates artists table - contains information about artists (including name and artist link)
        sql_create_tables.update({"artists": "CREATE TABLE IF NOT EXISTS artists (\
                                    artist_id int PRIMARY KEY AUTO_INCREMENT,\
                                    artist_name varchar(255) UNIQUE,\
                                    artist_link varchar(255),\
                                    popularity INT,\
                                    followers_num INT\
                                    );"})

        # Creates publishers table - contains information about publishers (including name and link)
        sql_create_tables.update({"publishers": "CREATE TABLE IF NOT EXISTS publishers (\
                                      publisher_id int PRIMARY KEY AUTO_INCREMENT,\
                                      publisher_name varchar(255) UNIQUE,\
                                      publisher_link varchar(255)\
                                      );"})

        # Creates summaries table - contains information about summaries
        sql_create_tables.update({"summaries": "CREATE TABLE IF NOT EXISTS summaries (\
                                      summary_id int PRIMARY KEY AUTO_INCREMENT,\
                                      summary VARCHAR(767) UNIQUE\
                                      );"})

        # Creates albums table - contains information about albums (including name and other scraped information)
        sql_create_tables.update({"albums": "CREATE TABLE IF NOT EXISTS albums (\
                                  album_id int PRIMARY KEY AUTO_INCREMENT, \
                                  album_name varchar(255) UNIQUE, \
                                  album_link varchar(255), \
                                  details_and_credits_link varchar(255),\
                                  amazon_link TEXT,\
                                  release_date datetime,\
                                  summary_id int,\
                                  artist_id int,\
                                  publisher_id int,\
                                  FOREIGN KEY (summary_id) REFERENCES summaries(summary_id),\
                                  FOREIGN KEY (artist_id) REFERENCES artists(artist_id),\
                                  FOREIGN KEY (publisher_id) REFERENCES publishers(publisher_id)\
                                  );"})

        # Creates albums_to_genres table - functions as an intermediate table for the many-to-many relationship
        # between albums and genres
        sql_create_tables.update({"albums_to_genres": "CREATE TABLE IF NOT EXISTS albums_to_genres (\
                                  album_id int,\
                                  genre_id int,\
                                  PRIMARY KEY (album_id, genre_id),\
                                  FOREIGN KEY (genre_id) REFERENCES genres(genre_id),\
                                  FOREIGN KEY (album_id) REFERENCES albums(album_id)\
                                 );"})

        # Creates chart_history table - contains information about when the scrape occurred and what the
        # chart looked like at the time
        sql_create_tables.update({"chart_history": "CREATE TABLE IF NOT EXISTS chart_history (\
                                  scrape_id int PRIMARY KEY AUTO_INCREMENT,  \
                                  scrape_datetime datetime,\
                                  chart_id int,\
                                  album_id int,\
                                  album_rank int,\
                                  metascore int,\
                                  user_score float,\
                                  num_of_critic_reviews int,\
                                  num_of_user_reviews int,\
                                  FOREIGN KEY (chart_id) REFERENCES charts(chart_id),\
                                  FOREIGN KEY (album_id) REFERENCES albums(album_id)\
                                  );"})

        try:
            for table in sql_create_tables.keys():
                cursor.execute(sql_create_tables[table])
            cursor.execute("COMMIT")
            logging.info(f"Database was created successfully.")

        except Exception as e:
            logging.critical(f"Failed creating table: {table}. Exiting program.\n {e}")
            cursor.execute("rollback")


def update_charts_table(cursor, filter_by_arg, year_arg, sort_by_arg):
    """
    Take dictionary of scraped data and add relevant information to charts table in top_albums database
    :param cursor: cursor of pymysql.connect
    :param filter_by_arg: a string with the filter method used
    :param year_arg: a string with the year used in the filter
    :param sort_by_arg: a string with the sorting method used
    :return: cursor of pymysql.connect
    :return: chart_id
    """

    # Find if chart already exists
    query = "SELECT chart_id FROM charts WHERE filter_by = (%s) AND year = (%s) AND sort_by = (%s)"
    cursor.execute(query, (filter_by_arg, year_arg, sort_by_arg))
    chart_id = cursor.fetchone()

    # Insert if not exists
    if chart_id is None:
        # sql command to add a record into the charts table
        sql = "INSERT INTO charts (filter_by, year, sort_by) VALUES (%s, %s, %s)"
        cursor.execute(sql, (filter_by_arg, year_arg, sort_by_arg))
        chart_id = cursor.lastrowid
    else:
        chart_id = chart_id['chart_id']

    return cursor, chart_id


def update_albums_table(cursor, row, artist_id, publisher_id, summary_id):
    """
    Take dictionary of scraped data and add relevant information to albums table in top_albums database
    :param cursor: cursor of pymysql.connect
    :param row: a Series with the scrapped information of an album from the chart
    :param artist_id:
    :param publisher_id:
    :param summary_id:
    :return: cursor: cursor of pymysql.connect
    :return: album_id:
    """

    # Find if album already exists
    query = "SELECT album_id FROM albums WHERE artist_id = (%s) AND album_name = (%s)"
    cursor.execute(query, (artist_id, row['Album']))
    album_id = cursor.fetchone()

    # Insert if not exists
    if album_id is None:
        # sql command to add a record into albums table with name, link to Metacritic album page, link to Metacritic
        # page with more album details, link to purchase album on Amazon, and album release date.
        query = "INSERT INTO albums (album_name, album_link, details_and_credits_link, amazon_link, " \
                "release_date, artist_id, publisher_id, summary_id) " \
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) " \
                "ON DUPLICATE KEY UPDATE album_name=album_name"
        cursor.execute(query, (row['Album'], row['Link to Album Page'], row['Link to More Details and Album Credits'],
                               row['Amazon Link'], row['Release Date'],
                               artist_id, publisher_id, summary_id))
        album_id = cursor.lastrowid
    else:
        album_id = album_id['album_id']

    return cursor, album_id


def update_artists_table(cursor, row):
    """
    Take dictionary of scraped data and add relevant information to artists table in top_albums database
    :param cursor: cursor of pymysql.connect
    :param row: a Series with the scrapped information of an album from the chart
    :return: cursor: cursor of pymysql.connect
    :return: artist_id
    """

    # Find if artist already exists
    query = "SELECT artist_id FROM artists WHERE artist_name = (%s)"
    cursor.execute(query, (row['Artist']))
    artist_id = cursor.fetchone()

    # Insert if not exists
    if artist_id is None:
        # sql command to add a record into artists table with name and link to Metacritic artist page
        query = "INSERT INTO artists (artist_name, artist_link, popularity, followers_num) VALUES (%s, %s, %s, %s)"
        cursor.execute(query, (row['Artist'], row['Link to Artist Page'],
                               row['Spotify Artist Popularity'], row['Number of Spotify Followers']))
        artist_id = cursor.lastrowid
    else:
        artist_id = artist_id['artist_id']

    return cursor, artist_id


def update_summaries_table(cursor, row):
    """
    Take dictionary of scraped data and add relevant information to summaries table in top_albums database
    :param cursor: cursor of pymysql.connect
    :param row: a Series with the scrapped information of an album from the chart
    :return: cursor: cursor of pymysql.connect
    :return: summary_id
    """

    # Find if summary already exists
    query = "SELECT summary_id FROM summaries WHERE summary = (%s)"
    cursor.execute(query, (row['Summary']))
    summary_id = cursor.fetchone()

    # Insert if not exists
    if summary_id is None:
        # sql command to add a record into summaries table with album's summary
        query = "INSERT INTO summaries (summary) VALUES (%s)"
        cursor.execute(query, (row['Summary']))
        summary_id = cursor.lastrowid
    else:
        summary_id = summary_id['summary_id']

    return cursor, summary_id


def update_publishers_table(cursor, row):
    """
    Take dictionary of scraped data and add relevant information to publishers table in top_albums database
    :param cursor: cursor of pymysql.connect
    :param row: a Series with the scrapped information of an album from the chart
    :return: cursor: cursor of pymysql.connect
    :return: publisher_id
    """

    # Find if publisher already exists
    query = "SELECT publisher_id FROM publishers WHERE publisher_name = (%s)"
    cursor.execute(query, (row['Publisher']))
    publisher_id = cursor.fetchone()

    # Insert if not exists
    if publisher_id is None:
        # sql command to add a record into the publishers table with name and link to Metacritic publisher page
        query = "INSERT INTO publishers (publisher_name, publisher_link) VALUES (%s, %s) " \
                "ON DUPLICATE KEY UPDATE publisher_name=publisher_name"
        cursor.execute(query, (row['Publisher'], row['Link to Publisher Page']))
        publisher_id = cursor.lastrowid
    else:
        publisher_id = publisher_id['publisher_id']

    return cursor, publisher_id


def update_genres_table(cursor, row):
    """
    Take dictionary of scraped data and add relevant information to genres table in top_albums database
    :param cursor: cursor of pymysql.connect
    :param row: a Series with the scrapped information of an album from the chart
    :return: cursor: cursor of pymysql.connect
    :return: genre_id
    """
    genre_id_list = list()
    for genre in row['Album Genres']:

        # Find if genre already exists
        query = "SELECT genre_id FROM genres WHERE genre_name = (%s)"
        cursor.execute(query, genre)
        genre_id = cursor.fetchone()

        # Insert if not exists
        if genre_id is None:
            # sql command to add a genre into genres table
            query = "INSERT INTO genres (genre_name) VALUES (%s) "
            cursor.execute(query, genre)
            genre_id_list.append(cursor.lastrowid)
        else:
            genre_id_list.append(genre_id['genre_id'])

    return cursor, genre_id_list


def update_albums_to_genres(cursor, album_id, genre_id_list):
    """
    Take dictionary of scraped data and add relevant information to genres table in top_albums database
    :param cursor: cursor of pymysql.connect
    :param album_id:
    :param genre_id_list:
    :return: cursor: cursor of pymysql.connect
    """

    for genre_id in genre_id_list:

        # Find if album_id and genre_id already exists
        query = "SELECT * FROM albums_to_genres WHERE album_id = (%s) AND genre_id = (%s)"
        cursor.execute(query, (album_id, genre_id))
        genre_album = cursor.fetchone()

        # Insert if not exists
        if genre_album is None:
            # sql command to add album_id and genre_id into albums_to_genres table
            query = "INSERT INTO albums_to_genres (album_id, genre_id) VALUES (%s, %s)"
            cursor.execute(query, (album_id, genre_id))

    return cursor


def update_chart_history_table(cursor, row, chart_id, album_id):
    """
    Take dictionary of scraped data and add relevant information to chart_history table in top_albums database
    :param cursor: cursor of pymysql.connect
    :param row: a Series with the scrapped information of an album from the chart
    :param chart_id:
    :param album_id:
    :return: cursor: cursor of pymysql.connect
    """

    # SQL command to insert scrape datetime, album rank, metascore, user score, number of critic reviews and
    # number of user reviews for each album found on the chart page scraped
    sql_add_history = """INSERT INTO chart_history
                        (scrape_datetime, chart_id, album_id, album_rank, metascore, user_score,
                        num_of_critic_reviews, num_of_user_reviews)
                        VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s)"""

    cursor.execute(sql_add_history, (chart_id, album_id, row['Album Rank'], row['Metascore'], row['User Score'],
                                     row['No. of Critic Reviews'], row['No. of User Reviews']))

    return cursor


def add_data(albums_df, login_info, filter_by_arg, year_arg, sort_by_arg):
    """
    main function, will execute all the necessary functions to add scraped data to database in the appropriate positions
    :param albums_df:
    :param login_info:
    :param filter_by_arg:
    :param year_arg:
    :param sort_by_arg:
    """

    # Connect host
    with connect_to_db(login_info, database='top_albums') as cursor:

        # Use top_albums database
        sql_use = "USE top_albums"
        cursor.execute(sql_use)
        cursor.execute("COMMIT")

        try:

            # Update charts table
            cursor, chart_id = update_charts_table(cursor, filter_by_arg, year_arg, sort_by_arg)

            for index, row in albums_df.iterrows():
                # Update artists table
                cursor, artist_id = update_artists_table(cursor, row)

                # Update publishers table
                cursor, publisher_id = update_publishers_table(cursor, row)

                # Update summaries table
                cursor, summary_id = update_summaries_table(cursor, row)

                # Update genres table
                cursor, genre_id_list = update_genres_table(cursor, row)

                # Update albums table
                cursor, album_id = update_albums_table(cursor, row, artist_id, publisher_id, summary_id)

                # Update albums_to_genres table
                cursor = update_albums_to_genres(cursor, album_id, genre_id_list)

                # Update chart_history table
                cursor = update_chart_history_table(cursor, row, chart_id, album_id)

            # Commit
            cursor.execute("COMMIT")
            logging.info("Database was updated successfully.")

        except Exception as e:
            logging.critical(f"Failed updating database. Exiting program.{e}\n")
            cursor.execute("rollback")
