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

import grequests
from bs4 import BeautifulSoup
import pandas as pd
import config as cfg
import logging
import argparse
import os
from argparse import RawTextHelpFormatter
import sys
import connection
from datetime import datetime
import add_data_to_db
import create_database
import json

# Logging definition
if cfg.LOGFILE_DEBUG:
    logging.basicConfig(filename=cfg.LOGFILE_NAME, format="%(asctime)s %(levelname)s: %(message)s",
                        level=logging.DEBUG)
else:
    logging.basicConfig(filename=cfg.LOGFILE_NAME, format="%(asctime)s %(levelname)s: %(message)s",
                        level=logging.INFO)


def save_csv(args, albums_df):
    """
    save_csv() gets a Dataframe with the albums information and saves it to csv file.
    If the folder cfg.DATA_FOLDER does not exist, it creates it
    :param args: a Struct with all the input arguments of the py file
    :param albums_df: a Dataframe with the albums information
    """

    # Creates folder if not exists
    if not os.path.exists(cfg.DATA_FOLDER):
        os.mkdir(cfg.DATA_FOLDER)

    # File name is according to the current we're scraping chart
    file_name = args.sort + '_' + args.filter + '_' + args.year + '.csv'
    fullname = os.path.join(cfg.DATA_FOLDER, file_name)

    # Saves the chart information to csv file
    albums_df.to_csv(fullname)
    logging.info(f"CSV file was created. Initial information added.")


def use_grequests(page_url):
    """
    The function gets a list of urls and gets the page/s html
    :param page_url: a list of link/s to required web page/s
    :return: a list of strings with html of the page/s
    """

    # Creates a generator of grequests from the page_url list and gets the html/s
    rs = (grequests.get(u, headers={'User-Agent': 'Mozilla/5.0'}) for u in page_url)
    pages = grequests.map(rs)

    # Check if the request status is valid
    soups = []
    for i, page in enumerate(pages):
        try:  # check if there was a successful response
            if cfg.REQ_STATUS_LOWER <= page.status_code <= cfg.REQ_STATUS_UPPER:
                logging.info(f"{page_url[i]} was requested successfully.")
            else:
                logging.critical(f"{page_url[i]} was not requested successfully. Exiting program.")
                raise ValueError(f'The link was not valid for scraping\n{page_url[i]}')
        except AttributeError:
            logging.critical(f"{page_url[i]} was not requested successfully. Exiting program.")
            raise AttributeError(f'The link was not valid for scraping\n{page_url[i]}')
        soups.append(BeautifulSoup(page.content, 'html.parser'))

    return soups


def scrape_album_page(args, pages_url):
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
    :param args: a Struct with all the input arguments of the py file
    :param pages_url: a list with albums' url pages
    :returns a dictionary with information from all the albums' url pages
    """
    logging.debug(f"scrape_album_page() started")

    # According to the configuration, we create batch size
    if args.batch > 0:
        logging.debug(f"According to the configuration, we create batch size")
        pages_url_batch = [pages_url[i:i + args.batch] for i in range(0, len(pages_url), args.batch)]
    else:
        logging.critical(f"Batch size is {args.batch} but it must be greater than 0. Exiting program.")
        raise ValueError(f'Batch size is {args.batch} but it must be greater than 0')

    # Build the dictionary of details we're scraping from each individual album page
    album_details_dict = {}

    # iterate over urls found on main page
    current_progress = 0
    for pages_url in pages_url_batch:

        # Getting pages html
        soup_list = use_grequests(pages_url)

        # Prints Scraping Progress when the flag args.progress is true
        if len(pages_url_batch) > 0 and args.progress:
            total_progress = len(pages_url_batch) * args.batch
            current_progress += len(pages_url)
            print(f'Scraping Progress: {round(100 * current_progress / total_progress, 2)}%')

        count_batch_url = 0
        for soup_num, soup in enumerate(soup_list):

            # Prints url list when the flag args.url is true
            if args.url:
                print(pages_url[count_batch_url])
                count_batch_url += 1

            # Scraping additional details and adding them to dictionary
            # Scraping the link to the artist page
            album_details_dict.setdefault('Link to Artist Page', []).append(
                cfg.SITE_ADDRESS + soup.find('div', class_='product_artist').a['href'])

            # Scraping the publisher name and link to the publisher's Metacritic page
            publisher_html = soup.find('span', class_='data', itemprop='publisher')
            album_details_dict.setdefault('Publisher', []).append(publisher_html.a.span.text.strip())
            album_details_dict.setdefault('Link to Publisher Page', []).append(
                cfg.SITE_ADDRESS + publisher_html.a['href'].lstrip("['").rstrip("']"))

            # Scraping the link to the image of the album cover
            album_details_dict.setdefault('Album Cover Image', []).append(
                soup.find('img', class_='product_image large_image')['src'])

            # Scraping the genres listed on the album
            genres = soup.find('li', class_='summary_detail product_genre')
            if genres:
                genres = [genre.text for genre in genres.findAll('span') if "Genre(s)" not in genre.text]
                album_details_dict.setdefault('Album Genres', []).append(genres)
                # album_details_dict.setdefault('Album Genres', []).append(', '.join(genres))
            else:
                logging.warning(f"There is no genre define in the album's page{pages_url[soup_num]}")
                album_details_dict.setdefault('Album Genres', []).append('')

            # Scraping number of critic reviews and the link to the critic review page
            album_details_dict.setdefault('No. of Critic Reviews', []).append(
                soup.find('span', itemprop="reviewCount").text.strip())
            album_details_dict.setdefault('Link to Critic Reviews', []).append(
                cfg.SITE_ADDRESS + soup.find('li', class_="nav nav_critic_reviews").span.span.a["href"])

            # Scraping number of user reviews
            # If there is no number of user scores, add an empty cell
            try:
                user_score_html = soup.find('div', class_="userscore_wrap feature_userscore")
                album_details_dict.setdefault('No. of User Reviews', []).append(
                    int(user_score_html.find('span', class_='count').a.text.rstrip(' Ratings')))
            except AttributeError:
                logging.warning(
                    f"There was no number of user reviews found on {pages_url[soup_num]}. Added an empty cell instead.")
                album_details_dict.setdefault('No. of User Reviews', []).append(0)

            # Scraping link to the user review page
            album_details_dict.setdefault('Link to User Reviews', []).append(
                cfg.SITE_ADDRESS + soup.find('li', class_='nav nav_user_reviews').span.span.a["href"])

            # Scraping the link to page with more album details and album credits
            album_details_dict.setdefault('Link to More Details and Album Credits', []).append(
                cfg.SITE_ADDRESS + soup.find('li', class_="nav nav_details last_nav").span.span.a["href"])

            # Scraping the link to the Amazon page to buy the album
            # If there is no Amazon link, add an empty cell
            buy_album_link = soup.find('td', class_="esite_img_wrapper")
            if buy_album_link:
                album_details_dict.setdefault('Amazon Link', []).append(buy_album_link.a["href"])
            else:
                logging.warning(
                    f"There was no Amazon link found on {pages_url[soup_num]}. Added an empty cell instead.")
                album_details_dict.setdefault('Amazon Link', []).append('')

    return album_details_dict


def scrape_chart_page(args, chart_url):
    """
    Take given url and scrape:
    * Album name
    * Artist name
    * Album release date
    * Metascore
    * User score
    * Link to individual album page
    :param args: a Struct with all the input arguments of the py file
    :param chart_url: a string of the chart url page
    :returns a dictionary with information of albums from the chart's url page
    """
    logging.debug(f"scrape_chart_page() started")

    # Test input validation
    if args.max is not None and args.max <= 0:
        logging.critical(f"args.max is {args.max} but it must be greater than 0. Exiting program.")
        raise ValueError(f'args.max is {args.max} but it must be greater than 0')

    # Getting chart html
    soup = use_grequests([chart_url])[0]

    #  Scraping album name
    album_name_text = soup.find_all('a', class_='title')
    album_names = [i.find("h3").get_text() for n, i in enumerate(album_name_text) if args.max is None or n < args.max]

    # Scraping ranks
    ranks_text = soup.find_all('span', class_='title numbered')
    ranks = [i.get_text().lstrip(cfg.STRIP_BEG).rstrip(cfg.STRIP_END).rstrip('.') for n, i in enumerate(ranks_text)
             if args.max is None or n < args.max]

    # Scraping artist name
    artist_name_text = soup.find_all('div', class_='artist')
    artist_names = [i.get_text().lstrip(cfg.STRIP_BEG).rstrip(cfg.STRIP_END) for n, i in enumerate(artist_name_text)
                    if args.max is None or n < args.max]

    # Scraping critic score (Metascore)
    metascore_text = soup.find_all('div', class_=lambda value: value and value.startswith('metascore_w large'))
    if len(metascore_text) == len(artist_names) * 2:
        metascore_text = metascore_text[::cfg.SCORE_INC]
    metascores = [i.get_text() for n, i in enumerate(metascore_text) if args.max is None or n < args.max]

    # Scraping user score
    userscore_text = soup.find_all('div', class_=lambda value: value and value.startswith('metascore_w user'))
    if len(userscore_text) == len(artist_names) * 2:
        userscore_text = userscore_text[::cfg.SCORE_INC]
    userscore_strings = [i.get_text() for n, i in enumerate(userscore_text) if args.max is None or n < args.max]
    userscores = []
    for userscore in userscore_strings:
        try:
            score_float = float(userscore)
            userscores.append(score_float)
        except ValueError:
            score_float = 0.0
            userscores.append(score_float)
            logging.warning(f"User score was not found")

    # Scraping release dates
    release_date_text = soup.find_all("div", class_="clamp-details")
    release_dates = [datetime.strptime(i.find("span").get_text(), "%B %d, %Y") for n, i in enumerate(release_date_text)
                     if args.max is None or n < args.max]

    # Scraping album descriptions
    descriptions_text = soup.find_all("div", class_="summary")
    summaries = [i.get_text().lstrip(cfg.STRIP_BEG).rstrip(cfg.STRIP_END) for n, i in enumerate(descriptions_text)
                 if args.max is None or n < args.max]

    # Scraping links to individual album pages (for use later)
    links = [(cfg.SITE_ADDRESS + i["href"]) for n, i in enumerate(album_name_text) if args.max is None or n < args.max]

    # Build initial dictionary with preliminary information (info you can find on the main chart page)
    return ({"Album": album_names,
             "Album Rank": ranks,
             "Artist": artist_names,
             "Metascore": metascores,
             "User Score": userscores,
             "Release Date": release_dates,
             "Summary": summaries,
             "Link to Album Page": links})


def scrape(args, login_info):
    """
    Takes given chart link and scrape relevant information of the albums
    :param args: a Struct with all the input arguments of the py file
    :param login_info: a dictionary with the user's login information
    """
    logging.debug(f"scrape() started")
    # Create the chart url
    chart_url = cfg.SITE_ADDRESS + cfg.SORT_BY[args.sort] + cfg.FILTER_BY[args.filter] + cfg.YEAR_RELEASE[args.year]
    print(f'main url: {chart_url}')

    # Scrape the chart page
    albums_dict = scrape_chart_page(args, chart_url)

    # Update dictionary with results of individual album page scraping
    albums_dict.update(scrape_album_page(args, albums_dict["Link to Album Page"]))

    # Turn dictionary with all details into DataFrame (can be removed if pandas is forbidden)
    albums_df = pd.DataFrame(albums_dict)
    print(albums_df)

    # Create csv file from DataFrame (for better organization)
    if args.save:
        save_csv(args, albums_df)

    # Adding data to Database
    add_data_to_db.add_data(albums_df, login_info)

    logging.info(f"Scraping information from {chart_url} and all the albums urls was done successfully")


def parse_args(args_string_list):
    """
    parse_args() is parsing the py file input arguments into the struct args
    :param args_string_list: a list of the input arguments of the py file
    :return a Struct with all the input arguments of the py file
    """
    logging.debug(f"parse_args() started")

    # Interface definition
    parser = argparse.ArgumentParser(description="This program scrapes data from Metacritic's albums charts.\n"
                                                 "It stores the data in relational database.\n"
                                                 "It allows to filter the desire chart by criteria.",
                                     formatter_class=RawTextHelpFormatter)

    subparser = parser.add_subparsers(dest='command')

    login = subparser.add_parser('login', help=f'Update login information. "login -h" for more information')
    login.add_argument('--username', type=str, required=True)
    login.add_argument('--password', type=str, required=True)

    settings = subparser.add_parser('settings', help=f'Change Settings. "settings -h" for more information')
    settings.add_argument('-i', '--init', help=f'Initiates the database', action='store_true')

    update = subparser.add_parser('update', help=f'Update database. "update -h" for more information')
    update.add_argument('-f', '--filter', type=str, required=True, help=f'Filter albums: {list(cfg.FILTER_BY.keys())}')
    update.add_argument('-y', '--year', type=str, required=True,
                        help=f'Albums year release: {min(cfg.YEAR_RELEASE.keys())} to {max(cfg.YEAR_RELEASE.keys())}')
    update.add_argument('-s', '--sort', type=str, required=True, help=f'Sort albums: {list(cfg.SORT_BY.keys())}')
    update.add_argument('-b', '--batch', type=int, help='grequest batch size', default=1)
    update.add_argument('-m', '--max', type=int, help="Maximum number of albums to scrape")
    update.add_argument('-p', '--progress', help=f'Shows scraping progress', action='store_true')
    update.add_argument('-u', '--url', help=f'Shows scraped urls', action='store_true')
    update.add_argument('-S', '--save', help=f'Saves csv file with the data', action='store_true')

    return parser.parse_args(args_string_list)


def main():
    """
    main() getting the input arguments of the py file and calling scrape() with them
    main() also catches exceptions
    """
    # Get user arguments
    args = parse_args(sys.argv[1:])

    # Read login information from login.json
    with open("login.json", "r") as openfile:
        login_info = json.load(openfile)

    # Update user login information
    if args.command == 'login':
        if args.username:
            with open("login.json", "w") as outfile:
                login_info['username'] = args.username
                json.dump(login_info, outfile)

        # Update password login information
        if args.password:
            with open("login.json", "w") as outfile:
                login_info['password'] = args.password
                json.dump(login_info, outfile)
        return

    # Run scrape() by user's criteria
    if args.command == 'update':
        try:
            scrape(args, login_info)
        except ValueError as e:
            print(e)
        except AttributeError as e:
            print(e)
        except TypeError as e:
            print(e)
        return

    if args.command == 'settings':
        if args.init:
            create_database.create_top_albums_db(login_info)


if __name__ == '__main__':
    main()

# TODO: add separate album id, change albums to chart history
# TODO: add column to chart history - source of scrape (add to summary_dict)
# TODO: add rank on chart page (add to summary_dict)
# TODO: update README
