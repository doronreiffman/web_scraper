import connection

def add_data(summary_dict, login_info, filter_by_arg, year_arg, sort_by_arg):
    with connection.connect_to_db(login_info) as cursor:
        sql_use = "USE top_albums"
        cursor.execute(sql_use)

        sql_add_history = "INSERT INTO chart_history (scrape_datetime, album_rank, metascore, user_score, num_of_critic_reviews, \
                          num_of_user_reviews) VALUES (NOW(), %s, %s, %s, %s, %s)"
        counter = 0
        for album_rank, metascore, userscore, num_of_critic_reviews, num_of_user_reviews \
                in zip(summary_dict['Album Rank'], summary_dict['Metascore'], summary_dict['User Score'],
                       summary_dict['No. of Critic Reviews'], summary_dict['No. of User Reviews']):
            cursor.execute(sql_add_history, (album_rank, metascore, userscore,
                                             num_of_critic_reviews, num_of_user_reviews))
            if counter == 0:
                id_counter = cursor.lastrowid
                counter += 1

        cursor.execute("COMMIT")

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

        cursor.execute("COMMIT")

