import pymysql.cursors


def connect_to_db(login_info):
    """
  function that connects to top_albums database
  """

    connect = pymysql.connect(host='localhost',
                              user=login_info['username'],
                              password=login_info['password'],
                              database='top_albums',
                              cursorclass=pymysql.cursors.DictCursor)
    return connect.cursor()


def create_top_albums_db(login_info):
    connect = pymysql.connect(host='localhost',
                              user=login_info['username'],
                              password=login_info['password'],
                              cursorclass=pymysql.cursors.DictCursor)
    with connect:
        with connect.cursor() as cursor:
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


def update_chart_history_table(cursor, summary_dict):
    # TODO: docstring

    sql_add_history = "INSERT INTO chart_history (scrape_datetime, album_rank, metascore, user_score," \
                      " num_of_critic_reviews, num_of_user_reviews) VALUES (NOW(), %s, %s, %s, %s, %s)"
    counter = 0
    for album_rank, metascore, userscore, num_of_critic_reviews, num_of_user_reviews \
            in zip(summary_dict['Album Rank'], summary_dict['Metascore'], summary_dict['User Score'],
                   summary_dict['No. of Critic Reviews'], summary_dict['No. of User Reviews']):
        cursor.execute(sql_add_history, (album_rank, metascore, userscore,
                                         num_of_critic_reviews, num_of_user_reviews))
        if counter == 0:
            id_counter = cursor.lastrowid
            counter += 1

    return cursor, id_counter


def update_charts_table(cursor, id_counter, summary_dict, filter_by_arg, year_arg, sort_by_arg):
    # TODO: docstring
    sql_find_chart_id = "SELECT chart_id FROM charts WHERE filter_by = (%s) AND year = (%s) AND sort_by = (%s)"
    cursor.execute(sql_find_chart_id, (filter_by_arg, year_arg, sort_by_arg))
    correct_chart_id_fetch = cursor.fetchone()
    sql_add_charts = "INSERT INTO charts (filter_by, year, sort_by) VALUES (%s, %s, %s)"
    sql_add_chart_id_to_history = "UPDATE chart_history SET chart_id = (%s) WHERE scrape_id = (%s)"
    chart_increment_count = id_counter
    if not correct_chart_id_fetch:
        cursor.execute(sql_add_charts, (filter_by_arg, year_arg, sort_by_arg))
        last_chart_id = cursor.lastrowid
        for scrape in summary_dict['Album']:
            cursor.execute(sql_add_chart_id_to_history, (last_chart_id, chart_increment_count))
            chart_increment_count += 1
    else:
        for scrape in summary_dict['Album']:
            correct_chart_id = correct_chart_id_fetch['chart_id']
            cursor.execute(sql_add_chart_id_to_history, (correct_chart_id, chart_increment_count))
            chart_increment_count += 1
    return cursor


def update_albums_table(cursor, id_counter, summary_dict):
    # TODO: docstring
    sql_add_albums = "INSERT INTO albums (album_name, album_link, details_and_credits_link, amazon_link, " \
                     "release_date) " \
                     "VALUES (%s, %s, %s, %s, %s) " \
                     "ON DUPLICATE KEY UPDATE album_name=album_name"
    sql_add_album_id_to_history = "UPDATE chart_history SET album_id = (%s) WHERE scrape_id = (%s)"
    album_increment_count = id_counter
    for album_name, album_link, details_and_credits_link, amazon_link, release_date in \
            zip(summary_dict['Album'], summary_dict['Link to Album Page'],
                summary_dict['Link to More Details and Album Credits'], summary_dict['Amazon Link'],
                summary_dict['Release Date']):
        cursor.execute(sql_add_albums,
                       (album_name, album_link, details_and_credits_link, amazon_link, release_date))
        last_album_id = cursor.lastrowid
        if last_album_id:
            cursor.execute(sql_add_album_id_to_history, (last_album_id, album_increment_count))
        else:
            sql_find_album_id = "SELECT album_id from albums WHERE album_name = (%s)"
            cursor.execute(sql_find_album_id, album_name)
            correct_artist_id = cursor.fetchone()['album_id']
            cursor.execute(sql_add_album_id_to_history, (correct_artist_id, album_increment_count))
        album_increment_count += 1
    return cursor


def update_artists_table(cursor, id_counter, summary_dict):
    # TODO: docstring

    sql_add_artists = "INSERT INTO artists (artist_name, artist_link) VALUES (%s, %s) " \
                      "ON DUPLICATE KEY UPDATE artist_name=artist_name"
    sql_add_artist_id_to_history = "UPDATE chart_history SET artist_id = (%s) WHERE scrape_id = (%s)"
    artist_increment_count = id_counter
    for artist, artist_link in zip(summary_dict['Artist'], summary_dict['Link to Artist Page']):
        cursor.execute(sql_add_artists, (artist, artist_link))
        last_artist_id = cursor.lastrowid
        if last_artist_id:
            cursor.execute(sql_add_artist_id_to_history, (last_artist_id, artist_increment_count))
        else:
            sql_find_artist_id = "SELECT artist_id FROM artists WHERE artist_name = (%s)"
            cursor.execute(sql_find_artist_id, artist)
            correct_artist_id = cursor.fetchone()['artist_id']
            cursor.execute(sql_add_artist_id_to_history, (correct_artist_id, artist_increment_count))
        artist_increment_count += 1
    return cursor


def update_summaries_table(cursor, id_counter, summary_dict):
    # TODO: docstring

    sql_add_summaries = "INSERT INTO summaries (summary) VALUES (%s) " \
                        "ON DUPLICATE KEY UPDATE summary=summary"
    sql_add_summary_id_to_history = "UPDATE chart_history SET summary_id = (%s) WHERE scrape_id = (%s)"
    summary_increment_count = id_counter
    for summary in summary_dict['Summary']:
        cursor.execute(sql_add_summaries, summary)
        last_summary_id = cursor.lastrowid
        if last_summary_id:
            cursor.execute(sql_add_summary_id_to_history, (last_summary_id, summary_increment_count))
        else:
            sql_find_summary_id = "SELECT summary_id FROM summaries WHERE summary = (%s)"
            cursor.execute(sql_find_summary_id, summary)
            correct_summary_id = cursor.fetchone()['summary_id']
            cursor.execute(sql_add_summary_id_to_history, (correct_summary_id, summary_increment_count))
        summary_increment_count += 1
        return cursor


def update_publishers_table(cursor, id_counter, summary_dict):
    # TODO: docstring
    sql_add_publishers = "INSERT INTO publishers (publisher_name, publisher_link) VALUES (%s, %s) " \
                         "ON DUPLICATE KEY UPDATE publisher_name=publisher_name"
    publisher_increment_count = id_counter
    sql_add_publisher_id_to_history = "UPDATE chart_history SET publisher_id = (%s) WHERE scrape_id = (%s)"
    for publisher, publisher_link in zip(summary_dict['Publisher'], summary_dict['Link to Publisher Page']):
        cursor.execute(sql_add_publishers, (publisher, publisher_link))
        last_publisher_id = cursor.lastrowid
        if last_publisher_id:
            cursor.execute(sql_add_publisher_id_to_history, (last_publisher_id, publisher_increment_count))
        else:
            sql_find_publisher_id = "SELECT publisher_id FROM publishers WHERE publisher_name = (%s)"
            cursor.execute(sql_find_publisher_id, publisher)
            correct_publisher_id = cursor.fetchone()['publisher_id']
            cursor.execute(sql_add_publisher_id_to_history, (correct_publisher_id, publisher_increment_count))
        publisher_increment_count += 1
    return cursor


def update_genres_table(cursor, id_counter, summary_dict):
    # TODO: docstring

    sql_add_genres = "INSERT INTO genres (genre_name) VALUES (%s) ON DUPLICATE KEY UPDATE genre_name=genre_name"
    sql_add_foreign_albums_to_genres = "REPLACE INTO albums_to_genres (album_id, genre_id) VALUES (%s, %s)"
    for album, genre_list in zip(summary_dict['Album'], summary_dict['Album Genres']):
        sql_find_last_album_id = "SELECT album_id FROM ALBUMS WHERE album_name = (%s)"
        cursor.execute(sql_find_last_album_id, album)
        last_album_id = cursor.fetchone()['album_id']
        for genre in genre_list:
            cursor.execute(sql_add_genres, genre)
            last_genre_id = cursor.lastrowid
            if last_genre_id:
                cursor.execute(sql_add_foreign_albums_to_genres, (last_album_id, last_genre_id))
            else:
                sql_find_genre_id = "SELECT genre_id FROM genres WHERE genre_name = (%s)"
                cursor.execute(sql_find_genre_id, genre)
                correct_genre_id = cursor.fetchone()['genre_id']
                cursor.execute(sql_add_foreign_albums_to_genres, (last_album_id, correct_genre_id))
    return cursor


def add_data(summary_dict, login_info, filter_by_arg, year_arg, sort_by_arg):
    """
    TODO: docstring
    :param summary_dict:
    :param login_info:
    :param filter_by_arg:
    :param year_arg:
    :param sort_by_arg:
    :return:
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
        cursor = update_genres_table(cursor, id_counter, summary_dict)
        cursor.execute("COMMIT")
