"""
Takes given link and scrape relevant information from it.
Relevant information includes:
* Album name
* Artist name
* Album release date
* Metascore
* User score
* Link to individual album page
* Link to artist page
* Publisher name
* Link to publisher's Metacritic page
* Link to image of album cover
* Listed genres on album
* Number of critic reviews
* Link to critic review page
* Number of user reviews
* Link to user review page
* Link to page with additional details and album credits
* Link to Amazon purchase page
Author: Doron Reiffman & Yair Vagshal
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
import config as cfg
import logging
import connection
from datetime import datetime

if cfg.LOGFILE_DEBUG:
    logging.basicConfig(filename=cfg.LOGFILE_NAME, format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG)
else:
    logging.basicConfig(filename=cfg.LOGFILE_NAME, format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)


def use_requests(page_url):
    """
    The function gets a list of urls and gets the page/s html
    :param page_url: a list of link/s to required web page/s
    :return: a list of strings with html of the page/s
    """

    page = requests.get(page_url, headers={'User-Agent': 'Mozilla/5.0'})

    # Check if the request status is valid
    if page.status_code and cfg.REQ_STATUS_LOWER <= page.status_code <= cfg.REQ_STATUS_UPPER:
        logging.info(f"{page_url} was requested successfully.")
    else:
        logging.critical(f"{page_url} was not requested successfully. Exiting program.")
        raise ValueError(f'The link was not valid for scraping\n{page_url}')

    return BeautifulSoup(page.content, 'html.parser')


def scrape():
    """
    Take given url and scrape:
    * Album name
    * Artist name
    * Album release date
    * Metascore
    * User score
    * Link to individual album page

    The user can change cfg.URL_INDEX in order to scrape different urls from cfg.URL_LIST
    """
    logging.debug(f"scrape() started")

    # uncomment to delete database and create again
    # connection.create_top_albums_db()

    with connection.connect_to_db() as cursor:

        sql_commit = "COMMIT"

        # Getting page html - change URL_INDEX in config file to scrape a different URL in URL_LIST
        soup = use_requests(cfg.URL_LIST[cfg.URL_INDEX])

        #  Scraping album name
        album_name_text = soup.find_all('a', class_='title')
        album_names = [i.find("h3").get_text() for i in album_name_text]

        # Scraping links to individual album pages (for use later)
        links = [(cfg.SITE_ADDRESS + i["href"]) for i in album_name_text]

        # Scraping critic score (Metascore)
        metascore_text = soup.find_all('div', class_=lambda value: value and value.startswith('metascore_w large'))
        metascores = [i.get_text() for i in metascore_text[::cfg.METASCORE_INC]]

        # Scraping user score
        userscore_text = soup.find_all('div', class_=lambda value: value and value.startswith('metascore_w user'))
        userscore_strings = [i.get_text() for i in userscore_text]
        userscores = []
        for userscore in userscore_strings:
            try:
                score_float = float(userscore)
                userscores.append(score_float)
            except ValueError:
                score_float = None
                userscores.append(score_float)

        # Scraping release dates
        release_date_text = soup.find_all("div", class_="clamp-details")
        release_dates = [datetime.strptime(i.find("span").get_text(), "%B %d, %Y") for i in release_date_text]

        sql_add_albums = "INSERT INTO albums (album_name, album_link, metascore, user_score, release_date, " \
                         "scrape_date) VALUES (%s, %s, %s, %s, %s, NOW())"
        for album, link, metascore, userscore, release_date\
                in zip(album_names, links, metascores, userscores, release_dates):
            cursor.execute(sql_add_albums, (album, link, metascore, userscore, release_date))

        # Scraping artist name
        artist_name_text = soup.find_all('div', class_='artist')
        artist_names = [i.get_text().lstrip(cfg.STRIP_BEG).rstrip(cfg.STRIP_END) for i in artist_name_text]
        sql_add_artists = "INSERT INTO artists (artist_name) VALUES (%s) " \
                          "ON DUPLICATE KEY update artist_name=artist_name"
        sql_add_artist_id_to_albums = "UPDATE albums SET artist_id = (%s) WHERE album_id = (%s)"
        increment_count = 1
        for artist in artist_names:
            cursor.execute(sql_add_artists, artist)
            last_artist_id = cursor.lastrowid
            if last_artist_id:
                cursor.execute(sql_add_artist_id_to_albums, (last_artist_id, increment_count))
            else:
                sql_find_artist_id = "SELECT artist_id FROM artists WHERE artist_name = (%s)"
                cursor.execute(sql_find_artist_id, artist)
                correct_artist_id = cursor.fetchone()['artist_id']
                cursor.execute(sql_add_artist_id_to_albums, (correct_artist_id, increment_count))
            increment_count += 1

        # Scraping album descriptions
        descriptions_text = soup.find_all("div", class_="summary")
        summaries = [i.get_text().lstrip(cfg.STRIP_BEG).rstrip(cfg.STRIP_END) for i in descriptions_text]
        sql_add_summaries = "INSERT INTO summaries (summary) VALUES (%s)"
        sql_add_summary_id_to_albums = "UPDATE albums SET summary_id = (%s) WHERE album_id = (%s)"
        for summary in summaries:
            cursor.execute(sql_add_summaries, summary)
            last_id = cursor.lastrowid
            cursor.execute(sql_add_summary_id_to_albums, (last_id, last_id))

        cursor.execute(sql_commit)

        # foreign_key1 = "ALTER TABLE albums_to_genres\
        #                         ADD FOREIGN KEY (album_id)\
        #                         REFERENCES albums (album_id)"
        # foreign_key2 = "ALTER TABLE albums_to_genres\
        #                         ADD FOREIGN KEY (genre_id)\
        #                         REFERENCES genres (genre_id)"
        # foreign_key3 = "ALTER TABLE albums\
        #                         ADD FOREIGN KEY (artist_id)\
        #                         REFERENCES artists (artist_id)"
        # foreign_key4 = "ALTER TABLE albums\
        #                         ADD FOREIGN KEY (publisher_id)\
        #                         REFERENCES publishers (publisher_id)"
        # foreign_key5 = "ALTER TABLE albums\
        #                         ADD FOREIGN KEY (summary_id)\
        #                         REFERENCES summaries (summary_id)"
        # cursor.execute(foreign_key1)
        # cursor.execute(foreign_key2)
        # cursor.execute(foreign_key3)
        # cursor.execute(foreign_key4)
        # cursor.execute(foreign_key5)
        # cursor.execute(sql_commit)


    # Build initial dictionary with preliminary information (info you can find on the main chart page)
    summary_dict = ({"Album": album_names,
                     "Artist": artist_names,
                     "Metascore": metascores,
                     "User Score": userscores,
                     "Release Date": release_dates,
                     "Summary": summaries,
                     "Link to Album Page": links})


    scrape_album_page(links)

    # Turn dictionary with all details into DataFrame (can be removed if pandas is forbidden)
    top_albums = pd.DataFrame(summary_dict)

    # Create csv file from DataFrame (for better organization)
    top_albums.to_csv("top albums.csv")
    logging.info(f"CSV file was created. Initial information added.")


def scrape_album_page(pages_url):
    """
    Receives each page url from main chart page and scrapes additional details from given url:
    * Link to artist page
    * Publisher name
    * Link to publisher's Metacritic page
    * Link to image of album cover
    * Listed genres on album
    * Number of critic reviews
    * Link to critic review page
    * Number of user reviews
    * Link to user review page
    * Link to page with additional details and album credits
    * Link to Amazon purchase page
    """
    logging.debug(f"scrape_album_page() started")

    # Build the dictionary of details we're scraping from each individual album page
    album_details_dict = {}

    with connection.connect_to_db() as cursor:
        # Uses top_albums database
        sql_use = "USE top_albums"
        cursor.execute(sql_use)

        increment_count = 1
        # iterate over urls found on main page
        for url in pages_url:

            # Getting page html
            soup = use_requests(url)

            # Scraping additional details and adding them to dictionary
            # Scraping the link to the artist page
            artist_link = cfg.SITE_ADDRESS + soup.find('div', class_='product_artist').a['href']
            sql_add_link_to_artists = "UPDATE artists SET artist_link = (%s) WHERE artist_id = (%s)"
            cursor.execute(sql_add_link_to_artists, (artist_link, increment_count))

            # Scraping the publisher name and link to the publisher's Metacritic page
            publisher_html = soup.find('span', class_='data', itemprop='publisher')
            publisher_name = publisher_html.a.span.text.strip()
            publisher_link = cfg.SITE_ADDRESS + publisher_html.a['href'].lstrip("['").rstrip("']")
            sql_add_publishers = "INSERT INTO publishers (publisher_name, publisher_link) VALUES (%s, %s) ON " \
                                 "DUPLICATE KEY UPDATE publisher_name=publisher_name"
            sql_add_publisher_id_to_albums = "UPDATE albums SET publisher_id = (%s) WHERE album_id = (%s)"
            cursor.execute(sql_add_publishers, (publisher_name, publisher_link))
            last_publisher_id = cursor.lastrowid
            if last_publisher_id:
                cursor.execute(sql_add_publisher_id_to_albums, (last_publisher_id, increment_count))
            else:
                sql_find_publisher_id = "SELECT publisher_id FROM publishers WHERE publisher_name = (%s)"
                cursor.execute(sql_find_publisher_id, publisher_name)
                correct_publisher_id = cursor.fetchone()['publisher_id']
                cursor.execute(sql_add_publisher_id_to_albums, (correct_publisher_id, increment_count))

            # Scraping the link to the image of the album cover
            album_details_dict.setdefault('Album Cover Image', []).append(
                soup.find('img', class_='product_image large_image')['src'])

            # Scraping the genres listed on the album
            genres = soup.find('li', class_='summary_detail product_genre')
            genre_list = ' '.join([genre.text for genre in genres.findAll('span')]).lstrip("Genre(s): \n").split(' ')
            sql_add_genres = "INSERT INTO genres (genre_name) VALUES (%s) ON DUPLICATE KEY update genre_name=genre_name"
            sql_add_foreign_albums_to_genres = "INSERT INTO albums_to_genres (album_id, genre_id) VALUES (%s, %s)"
            for genre in genre_list:
                cursor.execute(sql_add_genres, genre)
                last_genre_id = cursor.lastrowid
                if last_genre_id:
                    cursor.execute(sql_add_foreign_albums_to_genres, (increment_count, last_genre_id))
                else:
                    sql_find_genre_id = "SELECT genre_id FROM genres WHERE genre_name = (%s)"
                    cursor.execute(sql_find_genre_id, genre)
                    correct_genre_id = cursor.fetchone()['genre_id']
                    cursor.execute(sql_add_foreign_albums_to_genres, (increment_count, correct_genre_id))

            # Scraping number of critic reviews and the link to the critic review page
            num_of_critic_reviews = soup.find('span', itemprop="reviewCount").text.strip()
            link_to_critic_reviews = cfg.SITE_ADDRESS + soup.find('li', class_="nav nav_critic_reviews").span.span.a["href"]
            sql_add_critic_reviews_to_albums = "UPDATE albums SET num_of_critic_reviews = (%s) WHERE album_id = (%s)"
            cursor.execute(sql_add_critic_reviews_to_albums, (num_of_critic_reviews, increment_count))

            # Scraping number of user reviews
            # If there is no number of user scores, add an empty cell
            sql_add_user_reviews_to_albums = "UPDATE albums SET num_of_user_reviews = (%s) WHERE album_id = (%s)"
            try:
                user_score_html = soup.find('div', class_="userscore_wrap feature_userscore")
                num_of_user_reviews = int(user_score_html.find('span', class_='count').a.text.rstrip(' Ratings'))
                cursor.execute(sql_add_user_reviews_to_albums, (num_of_user_reviews, increment_count))
            except AttributeError:
                logging.warning(f"There was no number of user reviews found on {url}. Added an empty cell instead.")
                cursor.execute(sql_add_user_reviews_to_albums, (None, increment_count))


            # Scraping link to the user review page
            album_details_dict.setdefault('Link to User Reviews', []).append(
                cfg.SITE_ADDRESS + soup.find('li', class_='nav nav_user_reviews').span.span.a["href"])

            # Scraping the link to page with more album details and album credits
            more_details_link = cfg.SITE_ADDRESS + soup.find('li', class_="nav nav_details last_nav").span.span.a["href"]
            sql_add_detail_link_to_albums = "UPDATE albums SET details_and_credits_link = (%s) WHERE album_id = (%s)"
            cursor.execute(sql_add_detail_link_to_albums, (more_details_link, increment_count))


            # Scraping the link to the Amazon page to buy the album
            # If there is no Amazon link, add an empty cell
            sql_add_amazon_link_to_albums = "UPDATE albums SET amazon_link = (%s) WHERE album_id = (%s)"
            try:
                buy_album_link = soup.find('td', class_="esite_img_wrapper").a["href"]
                cursor.execute(sql_add_amazon_link_to_albums, (buy_album_link, increment_count))
            except AttributeError:
                logging.warning(f"There was no Amazon link found on {url}. Added an empty cell instead.")
                cursor.execute(sql_add_amazon_link_to_albums, (None, increment_count))

            cursor.execute("COMMIT")
            increment_count += 1

    return album_details_dict


scrape()


# def main():
#     """
#     main() first calls test_use_requests to check that the page is being requested properly,
#     Then calls scrape() to scrape the desired page.
#     """
#     try:
#         scrape()
#     except ValueError as e:
#         print(e)

#
# if __name__ == '__main__':
#     main()
