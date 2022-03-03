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

    # Getting page html - change URL_INDEX in config file to scrape a different URL in URL_LIST
    soup = use_requests(cfg.URL_LIST[cfg.URL_INDEX])

    #  Scraping album name
    album_name_text = soup.find_all('a', class_='title')
    album_names = [i.find("h3").get_text() for i in album_name_text]

    # Scraping artist name
    artist_name_text = soup.find_all('div', class_='artist')
    artist_names = [i.get_text().lstrip(cfg.STRIP_BEG).rstrip(cfg.STRIP_END) for i in artist_name_text]

    # Scraping critic score (Metascore)
    metascore_text = soup.find_all('div', class_=lambda value: value and value.startswith('metascore_w large'))
    metascores = [i.get_text() for i in metascore_text[::cfg.METASCORE_INC]]

    # Scraping user score
    userscore_text = soup.find_all('div', class_=lambda value: value and value.startswith('metascore_w user'))
    userscores = [i.get_text() for i in userscore_text]

    # Scraping release dates
    release_date_text = soup.find_all("div", class_="clamp-details")
    release_dates = [i.find("span").get_text() for i in release_date_text]

    # Scraping album descriptions
    descriptions_text = soup.find_all("div", class_="summary")
    summaries = [i.get_text().lstrip(cfg.STRIP_BEG).rstrip(cfg.STRIP_END) for i in descriptions_text]

    # Scraping links to individual album pages (for use later)
    links = [(cfg.SITE_ADDRESS + i["href"]) for i in album_name_text]

    # Build initial dictionary with preliminary information (info you can find on the main chart page)
    summary_dict = ({"Album": album_names,
                     "Artist": artist_names,
                     "Metascore": metascores,
                     "User Score": userscores,
                     "Release Date": release_dates,
                     "Summary": summaries,
                     "Link to Album Page": links})

    # Update dictionary with results of individual album page scraping (see below)
    summary_dict.update(scrape_album_page(links))

    # Turn dictionary with all details into DataFrame (can be removed if pandas is forbidden)
    top_albums = pd.DataFrame(summary_dict)

    # Create csv file from DataFrame (for better organization)
    top_albums.to_csv("top albums.csv")
    logging.info(f"CSV file was created. Initial information added.")

    print(top_albums)


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

    # iterate over urls found on main page
    for url in pages_url:

        # Getting page html
        soup = use_requests(url)

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
        album_details_dict.setdefault('Album Genres', []).append(
            ' '.join([genre.text for genre in genres.findAll('span')]).lstrip("Genre(s): \n"))

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
                user_score_html.find('span', class_='count').a.text.rstrip(' Ratings'))
        except AttributeError:
            logging.warning(f"There was no number of user reviews found on {url}. Added an empty cell instead.")
            album_details_dict.setdefault('No. of User Reviews', []).append('')

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
            logging.warning(f"There was no Amazon link found on {url}. Added an empty cell instead.")
            album_details_dict.setdefault('Amazon Link', []).append('')

    return album_details_dict


def test_use_requests():
    # Tests that use_requests function properly catches an exception for bad links
    # Should add a debug log to logfile when in debug mode
    test_url = "https://www.metacritic.com/bad_link_for_testing"
    try:
        use_requests(test_url)
    except ValueError:
        logging.debug(f"test_use_requests(): Properly caught the exception for bad link: {test_url}")

    # Test that use_requests function does not catch an exception for a good link
    test_url = "https://www.metacritic.com/"
    use_requests(test_url)
    logging.debug(f"test_use_requests(): {test_url} was requested successfully")


def main():
    """
    main() first calls test_use_requests to check that the page is being requested properly,
    Then calls scrape() to scrape the desired page.
    """
    try:
        test_use_requests()
        scrape()
    except ValueError as e:
        print(e)


if __name__ == '__main__':
    main()
