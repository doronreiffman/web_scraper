import connection

def add_data(summary_dict):
    with connection.connect_to_db() as cursor:
        sql_use = "USE top_albums"
        cursor.execute(sql_use)

        sql_add_albums = "INSERT INTO albums (album_name, album_link, metascore, user_score, release_date, " \
                         "scrape_date) VALUES (%s, %s, %s, %s, %s, NOW())"
        for album, link, metascore, userscore, release_date\
                in zip(summary_dict['Album'], summary_dict['Link to Album Page'], summary_dict['Metascore'],
                       summary_dict['User Score'], summary_dict['Release Date']):
            cursor.execute(sql_add_albums, (album, link, metascore, userscore, release_date))

        cursor.execute("COMMIT")

        sql_find_first_id = "SELECT album_id from albums WHERE album_name = (%s) ORDER BY album_id DESC LIMIT 1"
        cursor.execute(sql_find_first_id, summary_dict['Album'][0])
        id_counter = cursor.fetchone()['album_id']
        print(summary_dict['Album'][0], id_counter)

        sql_add_artists = "INSERT INTO artists (artist_name, artist_link) VALUES (%s, %s) " \
                          "ON DUPLICATE KEY UPDATE artist_name=artist_name"
        sql_add_artist_id_to_albums = "UPDATE albums SET artist_id = (%s) WHERE album_id = (%s)"
        artist_increment_count = id_counter
        for artist, artist_link in zip(summary_dict['Artist'], summary_dict['Link to Artist Page']):
            cursor.execute(sql_add_artists, (artist, artist_link))
            last_artist_id = cursor.lastrowid
            if last_artist_id:
                cursor.execute(sql_add_artist_id_to_albums, (last_artist_id, artist_increment_count))
            else:
                sql_find_artist_id = "SELECT artist_id FROM artists WHERE artist_name = (%s)"
                cursor.execute(sql_find_artist_id, artist)
                correct_artist_id = cursor.fetchone()['artist_id']
                cursor.execute(sql_add_artist_id_to_albums, (correct_artist_id, artist_increment_count))
            artist_increment_count += 1

        sql_add_summaries = "INSERT INTO summaries (summary) VALUES (%s)"
        sql_add_summary_id_to_albums = "UPDATE albums SET summary_id = (%s) WHERE album_id = (%s)"
        for summary in summary_dict['Summary']:
            cursor.execute(sql_add_summaries, summary)
            last_id = cursor.lastrowid
            cursor.execute(sql_add_summary_id_to_albums, (last_id, last_id))

        foreign_key1 = "ALTER TABLE albums_to_genres\
                                ADD FOREIGN KEY (album_id)\
                                REFERENCES albums (album_id)"
        foreign_key2 = "ALTER TABLE albums_to_genres\
                                ADD FOREIGN KEY (genre_id)\
                                REFERENCES genres (genre_id)"
        foreign_key3 = "ALTER TABLE albums\
                                ADD FOREIGN KEY (artist_id)\
                                REFERENCES artists (artist_id)"
        foreign_key4 = "ALTER TABLE albums\
                                ADD FOREIGN KEY (publisher_id)\
                                REFERENCES publishers (publisher_id)"
        foreign_key5 = "ALTER TABLE albums\
                                ADD FOREIGN KEY (summary_id)\
                                REFERENCES summaries (summary_id)"
        cursor.execute(foreign_key1)
        cursor.execute(foreign_key2)
        cursor.execute(foreign_key3)
        cursor.execute(foreign_key4)
        cursor.execute(foreign_key5)
        cursor.execute("COMMIT")

        publisher_increment_count = id_counter
        sql_add_publishers = "INSERT INTO publishers (publisher_name, publisher_link) VALUES (%s, %s) ON " \
                             "DUPLICATE KEY UPDATE publisher_name=publisher_name"
        sql_add_publisher_id_to_albums = "UPDATE albums SET publisher_id = (%s) WHERE album_id = (%s)"
        for publisher, publisher_link in zip(summary_dict['Publisher'], summary_dict['Link to Publisher Page']):
            cursor.execute(sql_add_publishers, (publisher, publisher_link))
            last_publisher_id = cursor.lastrowid
            if last_publisher_id:
                cursor.execute(sql_add_publisher_id_to_albums, (last_publisher_id, publisher_increment_count))
            else:
                sql_find_publisher_id = "SELECT publisher_id FROM publishers WHERE publisher_name = (%s)"
                cursor.execute(sql_find_publisher_id, publisher)
                correct_publisher_id = cursor.fetchone()['publisher_id']
                cursor.execute(sql_add_publisher_id_to_albums, (correct_publisher_id, publisher_increment_count))
            publisher_increment_count += 1

        album_to_genre_increment_count = id_counter
        sql_add_genres = "INSERT INTO genres (genre_name) VALUES (%s) ON DUPLICATE KEY update genre_name=genre_name"
        sql_add_foreign_albums_to_genres = "INSERT INTO albums_to_genres (album_id, genre_id) VALUES (%s, %s)"
        for genre_list in summary_dict['Album Genres']:
            for genre in genre_list:
                cursor.execute(sql_add_genres, genre)
                last_genre_id = cursor.lastrowid
                if last_genre_id:
                    cursor.execute(sql_add_foreign_albums_to_genres, (album_to_genre_increment_count, last_genre_id))
                else:
                    sql_find_genre_id = "SELECT genre_id FROM genres WHERE genre_name = (%s)"
                    cursor.execute(sql_find_genre_id, genre)
                    correct_genre_id = cursor.fetchone()['genre_id']
                    cursor.execute(sql_add_foreign_albums_to_genres, (album_to_genre_increment_count, correct_genre_id))
            album_to_genre_increment_count += 1

        critic_review_increment_count = id_counter
        sql_add_critic_reviews_to_albums = "UPDATE albums SET num_of_critic_reviews = (%s) WHERE album_id = (%s)"
        for num_of_critic_reviews in summary_dict['No. of Critic Reviews']:
            cursor.execute(sql_add_critic_reviews_to_albums, (num_of_critic_reviews, critic_review_increment_count))
            critic_review_increment_count += 1

        user_review_increment_count = id_counter
        sql_add_user_reviews_to_albums = "UPDATE albums SET num_of_user_reviews = (%s) WHERE album_id = (%s)"
        for num_of_user_reviews in summary_dict['No. of User Reviews']:
            cursor.execute(sql_add_user_reviews_to_albums, (num_of_user_reviews, user_review_increment_count))
            user_review_increment_count += 1

        detail_link_increment_count = id_counter
        sql_add_detail_link_to_albums = "UPDATE albums SET details_and_credits_link = (%s) WHERE album_id = (%s)"
        for more_details_link in summary_dict['Link to More Details and Album Credits']:
            cursor.execute(sql_add_detail_link_to_albums, (more_details_link, detail_link_increment_count))
            detail_link_increment_count += 1

        amazon_link_increment_count = id_counter
        sql_add_amazon_link_to_albums = "UPDATE albums SET amazon_link = (%s) WHERE album_id = (%s)"
        for buy_album_link in summary_dict['Amazon Link']:
            cursor.execute(sql_add_amazon_link_to_albums, (buy_album_link, amazon_link_increment_count))
            amazon_link_increment_count += 1

        cursor.execute("COMMIT")