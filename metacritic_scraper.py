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

URL = "https://www.metacritic.com/browse/albums/score/metascore/all?sort=desc"
SITE_ADDRESS = "https://www.metacritic.com"


def scrape(page_url):
    """
    Take given url and scrape:
    * Album name
    * Artist name
    * Album release date
    * Metascore
    * User score
    * Link to individual album page
    """
    page = requests.get(page_url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(page.content, 'html.parser')
    # strings to strip from longer strings of text
    str_to_strip_from_beg = "\n                                    by "
    str_to_strip_from_end = "\n                                "

    #  Scraping album name
    album_name_text = soup.find_all('a', class_='title')
    album_names = [i.find("h3").get_text() for i in album_name_text]

    # Scraping artist name
    artist_name_text = soup.find_all('div', class_='artist')
    artist_names = [i.get_text().lstrip(str_to_strip_from_beg).rstrip(str_to_strip_from_end) for i in artist_name_text]

    # Scraping critic score (Metascore)
    metascore_text = soup.find_all('div', class_=lambda value: value and value.startswith('metascore_w large'))
    metascores = [i.get_text() for i in metascore_text[::2]]

    # Scraping user score
    userscore_text = soup.find_all('div', class_=lambda value: value and value.startswith('metascore_w user'))
    userscores = [i.get_text() for i in userscore_text]

    # Scraping release dates
    release_date_text = soup.find_all("div", class_="clamp-details")
    release_dates = [i.find("span").get_text() for i in release_date_text]

    # Scraping album descriptions
    descriptions_text = soup.find_all("div", class_="summary")
    summaries = [i.get_text().lstrip(str_to_strip_from_beg).rstrip(str_to_strip_from_end) for i in descriptions_text]

    # Scraping links to individual album pages (for use later)
    links = [(SITE_ADDRESS + i["href"]) for i in album_name_text]

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
    # Build the dictionary of details we're scraping from each individual album page
    album_details_dict = {}

    # iterate over
    for url in pages_url:

        # Getting page information
        source = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}).text
        soup = BeautifulSoup(source, 'html.parser')

        # Scraping additional details and adding them to dictionary
        # Scraping the link to the artist page
        album_details_dict.setdefault('Link to Artist Page', []).append(
            SITE_ADDRESS + soup.find('div', class_='product_artist').a['href'])

        # Scraping the publisher name and link to the publisher's Metacritic page
        publisher_html = soup.find('span', class_='data', itemprop='publisher')
        album_details_dict.setdefault('Publisher', []).append(publisher_html.a.span.text.strip())
        album_details_dict.setdefault('Link to Publisher Page', []).append(SITE_ADDRESS + publisher_html.a['href'])

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
            SITE_ADDRESS + soup.find('li', class_="nav nav_critic_reviews").span.span.a["href"])

        # Scraping number of user reviews 
        # If there is no number of user scores, add an empty cell
        try:
            user_score_html = soup.find('div', class_="userscore_wrap feature_userscore")
            album_details_dict.setdefault('No. of User Reviews', []).append(
                user_score_html.find('span', class_='count').a.text.rstrip(' Ratings'))
        except AttributeError:
            album_details_dict.setdefault('No. of User Reviews', []).append('')

        # Scraping link to the user review page
        album_details_dict.setdefault('Link to User Reviews', []).append(
            SITE_ADDRESS + soup.find('li', class_='nav nav_user_reviews').span.span.a["href"])

        # Scraping the link to page with more album details and album credits
        album_details_dict.setdefault('Link to More Details and Album Credits', []).append(
            SITE_ADDRESS + soup.find('li', class_="nav nav_details last_nav").span.span.a["href"])

        # Scraping the link to the Amazon page to buy the album
        # If there is no Amazon link, add an empty cell
        buy_album_link = soup.find('td', class_="esite_img_wrapper")
        if buy_album_link:
            album_details_dict.setdefault('Amazon Link', []).append(buy_album_link.a["href"])
        else:
            album_details_dict.setdefault('Amazon Link', []).append('')

    return album_details_dict


if __name__ == '__main__':
    scrape(URL)
