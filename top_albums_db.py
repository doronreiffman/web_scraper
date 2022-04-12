"""
File that contains functions related to creating, connecting to, and adding scraped data to a database
Authors: Yair Vagshal and Doron Reiffman
"""

import pymysql.cursors


def create_top_albums_db(login_info):
    """
    Creates a database called 'top_albums' using login info given as a parameter.
    top_albums will hold the tables required to store data scraped from the album charts on the
    website Metacritic.
    See the README file for information on the tables and their columns.
    """
    # connect to database using given login info
    connect = pymysql.connect(host='localhost',
                              user=login_info['username'],
                              password=login_info['password'],
                              cursorclass=pymysql.cursors.DictCursor)
    with connect:
        with connect.cursor() as cursor:
            # Delete top_albums database if it exists, and then create top_albums
            sql_drop = "DROP DATABASE IF EXISTS top_albums"
            cursor.execute(sql_drop)
            sql = "CREATE DATABASE top_albums"
            cursor.execute(sql)

            # Uses top_albums database
            sql_use = "USE top_albums"
            cursor.execute(sql_use)

            # Creates chart_history table - contains information about when the scrape occurred and what the
            # chart looked like at the time
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

            # Creates charts table - contains information about charts (defined by the filter method, the year, and the
            # sort method
            sql_create_table_charts = "CREATE TABLE charts (\
                                      chart_id int PRIMARY KEY AUTO_INCREMENT, \
                                      filter_by varchar(255), \
                                      year int, \
                                      sort_by varchar(255)\
                                      )"

            # Creates albums table - contains information about albums (including name and other scraped information)
            sql_create_table_albums = "CREATE TABLE albums (\
                                      album_id int PRIMARY KEY AUTO_INCREMENT, \
                                      album_name varchar(255) UNIQUE, \
                                      album_link varchar(255), \
                                      details_and_credits_link varchar(255),\
                                      amazon_link TEXT,\
                                      release_date datetime\
                                      )"

            # Creates genres table - contains information about genres
            sql_create_table_genres = "CREATE TABLE genres (\
                                      genre_id int PRIMARY KEY AUTO_INCREMENT,\
                                      genre_name varchar(255) UNIQUE\
                                      )"

            # Creates albums_to_genres table - functions as an intermediate table for the many-to-many relationship
            # between albums and genres
            sql_create_table_albums_to_genres = "CREATE TABLE albums_to_genres (\
                                                album_id int,\
                                                genre_id int,\
                                                PRIMARY KEY (album_id, genre_id)\
                                                )"

            # Creates artists table - contains information about artists (including name and artist link)
            sql_create_table_artists = "CREATE TABLE artists (\
                                        artist_id int PRIMARY KEY AUTO_INCREMENT,\
                                        artist_name varchar(255) UNIQUE,\
                                        artist_link varchar(255),\
                                        popularity INT,\
                                        followers_num INT\
                                        )"

            # Creates publishers table - contains information about publishers (including name and link)
            sql_create_table_publishers = "CREATE TABLE publishers (\
                                          publisher_id int PRIMARY KEY AUTO_INCREMENT,\
                                          publisher_name varchar(255) UNIQUE,\
                                          publisher_link varchar(255)\
                                          )"

            # Creates summaries table - contains information about summaries
            sql_create_table_summaries = "CREATE TABLE summaries (\
                                          summary_id int PRIMARY KEY AUTO_INCREMENT,\
                                          summary VARCHAR(767) UNIQUE\
                                          )"

            # execute sql commands to create tables
            cursor.execute(sql_create_table_history)
            cursor.execute(sql_create_table_charts)
            cursor.execute(sql_create_table_albums)
            cursor.execute(sql_create_table_genres)
            cursor.execute(sql_create_table_albums_to_genres)
            cursor.execute(sql_create_table_artists)
            cursor.execute(sql_create_table_publishers)
            cursor.execute(sql_create_table_summaries)

            cursor.execute("COMMIT")

            # Create necessary parent-child relationships between tables
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

            # Execute sql commands to create parent-child relationships, and commits them
            cursor.execute(foreign_key1)
            cursor.execute(foreign_key2)
            cursor.execute(foreign_key3)
            cursor.execute(foreign_key4)
            cursor.execute(foreign_key5)
            cursor.execute(foreign_key6)
            cursor.execute(foreign_key7)
            cursor.execute("COMMIT")


def connect_to_db(login_info):
    """
    Connect to created database using login info given (username and password).
    Returns the connection for use
    """

    connect = pymysql.connect(host='localhost',
                              user=login_info['username'],
                              password=login_info['password'],
                              database='top_albums',
                              cursorclass=pymysql.cursors.DictCursor)
    return connect.cursor()


def update_chart_history_table(cursor, summary_dict):
    """
    Take dictionary of scraped data and add relevant information to chart_history table in top_albums database.
    """
    # SQL command to insert scrape datetime, album rank, metascore, user score, number of critic reviews and
    # number of user reviews for each album found on the chart page scraped
    sql_add_history = "INSERT INTO chart_history (scrape_datetime, album_rank, metascore, user_score," \
                      " num_of_critic_reviews, num_of_user_reviews) VALUES (NOW(), %s, %s, %s, %s, %s)"

    counter = 0
    id_counter = None

    # Iterate over each relevant element in the dictionary of scraped data, adding them correspondingly to the database
    for album_rank, metascore, userscore, num_of_critic_reviews, num_of_user_reviews \
            in zip(summary_dict['Album Rank'], summary_dict['Metascore'], summary_dict['User Score'],
                   summary_dict['No. of Critic Reviews'], summary_dict['No. of User Reviews']):
        cursor.execute(sql_add_history, (album_rank, metascore, userscore,
                                         num_of_critic_reviews, num_of_user_reviews))

        # The first scrape_id of each chart scrape will be used later so we can add relevant information from other
        # elements of the dictionary into their appropriate rows in chart_history - finding the first scrape_id allows
        # us to properly count from the first scrape
        if counter == 0:
            id_counter = cursor.lastrowid
            counter += 1

    return cursor, id_counter


def update_charts_table(cursor, id_counter, summary_dict, filter_by_arg, year_arg, sort_by_arg):
    """
    Take dictionary of scraped data and add relevant information to charts table in top_albums database.
    """
    # Find if we've added the chart we're currently scraping already (defined by filter, year, and sort) to the charts
    # table already - and if so, find its chart_id
    sql_find_chart_id = "SELECT chart_id FROM charts WHERE filter_by = (%s) AND year = (%s) AND sort_by = (%s)"
    cursor.execute(sql_find_chart_id, (filter_by_arg, year_arg, sort_by_arg))
    correct_chart_id_fetch = cursor.fetchone()

    # sql command to add a record into the charts table
    sql_add_charts = "INSERT INTO charts (filter_by, year, sort_by) VALUES (%s, %s, %s)"

    # sql command to update chart_history with the proper chart_id
    sql_add_chart_id_to_history = "UPDATE chart_history SET chart_id = (%s) WHERE scrape_id = (%s)"
    chart_increment_count = id_counter

    # Only execute sql command to add a new row to charts if we didn't find a chart with the same definitions
    # as the current chart
    if not correct_chart_id_fetch:
        cursor.execute(sql_add_charts, (filter_by_arg, year_arg, sort_by_arg))
        last_chart_id = cursor.lastrowid

        # add the last added chart_id to the proper column in chart_history
        for i in summary_dict['Album']:
            cursor.execute(sql_add_chart_id_to_history, (last_chart_id, chart_increment_count))
            chart_increment_count += 1

    # If there was already a chart with the same definitions as the current chart, find its chart_id and add it to the
    # proper column in chart_history
    else:
        for i in summary_dict['Album']:
            correct_chart_id = correct_chart_id_fetch['chart_id']
            cursor.execute(sql_add_chart_id_to_history, (correct_chart_id, chart_increment_count))
            chart_increment_count += 1
    return cursor


def update_albums_table(cursor, id_counter, summary_dict):
    """
    Take dictionary of scraped data and add relevant information to albums table in top_albums database.
    """
    # sql command to add a record into the albums table with name, link to Metacritic album page, link to Metacritic
    # page with more album details, link to purchase album on Amazon, and album release date.
    # If we find an identical album name, don't add a new record
    sql_add_albums = "INSERT INTO albums (album_name, album_link, details_and_credits_link, amazon_link, " \
                     "release_date) " \
                     "VALUES (%s, %s, %s, %s, %s) " \
                     "ON DUPLICATE KEY UPDATE album_name=album_name"

    # sql command to update chart_history with the proper album_id
    sql_add_album_id_to_history = "UPDATE chart_history SET album_id = (%s) WHERE scrape_id = (%s)"

    # album_increment_count will count up from the first scrape_id so we can add the album_id to the proper record in
    # chart_history
    album_increment_count = id_counter

    # Execute sql command to insert a new record
    for album_name, album_link, details_and_credits_link, amazon_link, release_date in \
            zip(summary_dict['Album'], summary_dict['Link to Album Page'],
                summary_dict['Link to More Details and Album Credits'], summary_dict['Amazon Link'],
                summary_dict['Release Date']):
        cursor.execute(sql_add_albums,
                       (album_name, album_link, details_and_credits_link, amazon_link, release_date))
        last_album_id = cursor.lastrowid
        # if we added a new record (because we didn't find an identical album in the table), add its album_id to
        # the corresponding row in chart_history
        if last_album_id:
            cursor.execute(sql_add_album_id_to_history, (last_album_id, album_increment_count))

        # Otherwise, find the album_id of the identical album instead, and add that album_id to the corresponding row
        else:
            sql_find_album_id = "SELECT album_id from albums WHERE album_name = (%s)"
            cursor.execute(sql_find_album_id, album_name)
            correct_artist_id = cursor.fetchone()['album_id']
            cursor.execute(sql_add_album_id_to_history, (correct_artist_id, album_increment_count))

        album_increment_count += 1

    return cursor


def update_artists_table(cursor, id_counter, summary_dict):
    """
    Take dictionary of scraped data and add relevant information to artists table in top_albums database.
    """
    # sql command to add a record into the artists table with name and link to Metacritic artist page
    # If we find an identical artist name, don't add a new record
    sql_add_artists = "INSERT INTO artists (artist_name, artist_link, popularity, followers_num) VALUES " \
                      "(%s, %s, %s, %s) " \
                      "ON DUPLICATE KEY UPDATE artist_name=artist_name"

    # sql command to update chart_history with the proper album_id
    sql_add_artist_id_to_history = "UPDATE chart_history SET artist_id = (%s) WHERE scrape_id = (%s)"

    # artist_increment_count will count up from the first scrape_id so we can add the album_id to the proper record in
    # chart_history
    artist_increment_count = id_counter

    # Execute sql command to insert a new record
    for artist, artist_link, popularity, followers_num in zip(summary_dict['Artist'],
                                                              summary_dict['Link to Artist Page'],
                                                              summary_dict['Artist Popularity'],
                                                              summary_dict['Number of Followers']):
        cursor.execute(sql_add_artists, (artist, artist_link, popularity, followers_num))
        last_artist_id = cursor.lastrowid
        # if we added a new record (because we didn't find an identical artist in the table), add its artist_id to
        # the corresponding row in chart_history
        if last_artist_id:
            cursor.execute(sql_add_artist_id_to_history, (last_artist_id, artist_increment_count))

        # Otherwise, find the artist_id of the identical artist instead, and add that artist_id to the corresponding row
        else:
            sql_find_artist_id = "SELECT artist_id FROM artists WHERE artist_name = (%s)"
            cursor.execute(sql_find_artist_id, artist)
            correct_artist_id = cursor.fetchone()['artist_id']
            cursor.execute(sql_add_artist_id_to_history, (correct_artist_id, artist_increment_count))

        artist_increment_count += 1

    return cursor


def update_summaries_table(cursor, id_counter, summary_dict):
    """
    Take dictionary of scraped data and add relevant information to artists table in top_albums database.
    """
    # sql command to add a record into the summaries table
    sql_add_summaries = "INSERT INTO summaries (summary) VALUES (%s) " \
                        "ON DUPLICATE KEY UPDATE summary=summary"

    # sql command to update chart_history with the proper summary_id
    sql_add_summary_id_to_history = "UPDATE chart_history SET summary_id = (%s) WHERE scrape_id = (%s)"

    # summary_increment_count will count up from the first scrape_id so we can add the album_id to the proper record in
    # chart_history
    summary_increment_count = id_counter

    # Execute sql command to insert a new record
    for summary in summary_dict['Summary']:
        cursor.execute(sql_add_summaries, summary)
        last_summary_id = cursor.lastrowid

        # if we added a new record , add its summary_id to the corresponding row in chart_history
        if last_summary_id:
            cursor.execute(sql_add_summary_id_to_history, (last_summary_id, summary_increment_count))

        # Otherwise, find the correct summary_id instead, and add that summary_id to the corresponding row
        else:
            sql_find_summary_id = "SELECT summary_id FROM summaries WHERE summary = (%s)"
            cursor.execute(sql_find_summary_id, summary)
            correct_summary_id = cursor.fetchone()['summary_id']
            cursor.execute(sql_add_summary_id_to_history, (correct_summary_id, summary_increment_count))

        summary_increment_count += 1

    return cursor


def update_publishers_table(cursor, id_counter, summary_dict):
    """
    Take dictionary of scraped data and add relevant information to publishers table in top_albums database.
    """
    # sql command to add a record into the publishers table with name and link to Metacritic publisher page
    # If we find an identical publisher name, don't add a new record
    sql_add_publishers = "INSERT INTO publishers (publisher_name, publisher_link) VALUES (%s, %s) " \
                         "ON DUPLICATE KEY UPDATE publisher_name=publisher_name"

    # sql command to update chart_history with the proper publisher_id
    sql_add_publisher_id_to_history = "UPDATE chart_history SET publisher_id = (%s) WHERE scrape_id = (%s)"

    # publisher_increment_count will count up from the first scrape_id so we can add the album_id to the proper record
    # in chart_history
    publisher_increment_count = id_counter

    # Execute sql command to insert a new record
    for publisher, publisher_link in zip(summary_dict['Publisher'], summary_dict['Link to Publisher Page']):
        cursor.execute(sql_add_publishers, (publisher, publisher_link))
        last_publisher_id = cursor.lastrowid

        # if we added a new record (because we didn't find an identical publisher in the table), add its publisher_id to
        # the corresponding row in chart_history
        if last_publisher_id:
            cursor.execute(sql_add_publisher_id_to_history, (last_publisher_id, publisher_increment_count))

        # Otherwise, find the publisher_id of the identical publisher instead,
        # and add that publisher_id to the corresponding row
        else:
            sql_find_publisher_id = "SELECT publisher_id FROM publishers WHERE publisher_name = (%s)"
            cursor.execute(sql_find_publisher_id, publisher)
            correct_publisher_id = cursor.fetchone()['publisher_id']
            cursor.execute(sql_add_publisher_id_to_history, (correct_publisher_id, publisher_increment_count))

        publisher_increment_count += 1

    return cursor


def update_genres_table(cursor, summary_dict):
    """
    Take dictionary of scraped data and add relevant information to artists table in top_albums database.
    """
    # sql command to add a record into the genres table
    # If we find an identical genre, don't add a new record
    sql_add_genres = "INSERT INTO genres (genre_name) VALUES (%s) ON DUPLICATE KEY UPDATE genre_name=genre_name"

    # sql command to update albums_to_genres with the proper album_id and genre_id
    sql_add_foreign_albums_to_genres = "REPLACE INTO albums_to_genres (album_id, genre_id) VALUES (%s, %s)"

    # Execute sql command to insert album_id and genre_id into albums_to_genres table for each genre in an album's
    # list of genres
    for album, genre_list in zip(summary_dict['Album'], summary_dict['Album Genres']):
        sql_find_last_album_id = "SELECT album_id FROM ALBUMS WHERE album_name = (%s)"
        cursor.execute(sql_find_last_album_id, album)
        last_album_id = cursor.fetchone()['album_id']
        for genre in genre_list:
            cursor.execute(sql_add_genres, genre)
            last_genre_id = cursor.lastrowid

            # if we added a new genre (because an identical genre wasn't found), add the album_id and the genre_id
            # to albums_to_genres
            if last_genre_id:
                cursor.execute(sql_add_foreign_albums_to_genres, (last_album_id, last_genre_id))

            # Otherwise, find the identical genre_id and add it alongside the album_id to albums_to_genres
            else:
                sql_find_genre_id = "SELECT genre_id FROM genres WHERE genre_name = (%s)"
                cursor.execute(sql_find_genre_id, genre)
                correct_genre_id = cursor.fetchone()['genre_id']
                cursor.execute(sql_add_foreign_albums_to_genres, (last_album_id, correct_genre_id))

    return cursor


def add_data(summary_dict, login_info, filter_by_arg, year_arg, sort_by_arg):
    """
    main function, will execute all the necessary functions to add scraped data to database in the appropriate positions
    """

    # Connect host
    with connect_to_db(login_info) as cursor:

        # Use top_albums database
        sql_use = "USE top_albums"
        cursor.execute(sql_use)
        cursor.execute("COMMIT")

        # Update chart_history table
        cursor, id_counter = update_chart_history_table(cursor, summary_dict)
        cursor.execute("COMMIT")

        # Update charts table
        cursor = update_charts_table(cursor, id_counter, summary_dict, filter_by_arg, year_arg, sort_by_arg)

        # Update albums table
        cursor = update_albums_table(cursor, id_counter, summary_dict)

        # Update artists table
        cursor = update_artists_table(cursor, id_counter, summary_dict)

        # Update summaries table
        cursor = update_summaries_table(cursor, id_counter, summary_dict)

        # Update publishers table
        cursor = update_publishers_table(cursor, id_counter, summary_dict)

        # Update genres table
        cursor = update_genres_table(cursor, summary_dict)
        cursor.execute("COMMIT")
