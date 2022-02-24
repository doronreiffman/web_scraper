"""
Takes given link and scrape relevant information from it.
Relevant information includes:
* Album name
* Artist name
* Album release date
* Metascore
* User score
* Link to individual album page
Author: Doron Reiffman
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd


url = "https://www.metacritic.com/browse/albums/score/metascore/all?sort=desc"


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

    #  Album name and Link text
    album_name_text = soup.find_all('a', class_='title')
    album_names = [i.find("h3").get_text() for i in album_name_text]

    # Artist name
    artist_name_text = soup.find_all('div', class_='artist')
    artist_names = [i.get_text().lstrip(str_to_strip_from_beg).rstrip(str_to_strip_from_end) for i in artist_name_text]

    # Critic score (Metascore)
    metascore_text = soup.find_all('div', class_=lambda value: value and value.startswith('metascore_w large'))
    metascores = [i.get_text() for i in metascore_text[::2]]

    # User score
    userscore_text = soup.find_all('div', class_=lambda value: value and value.startswith('metascore_w user'))
    userscores = [i.get_text() for i in userscore_text]

    # Release dates
    release_date_text = soup.find_all("div", class_="clamp-details")
    release_dates = [i.find("span").get_text() for i in release_date_text]

    # Summaries
    summary_text = soup.find_all("div", class_="summary")
    summaries = [i.get_text().lstrip(str_to_strip_from_beg).rstrip(str_to_strip_from_end) for i in summary_text]

    # Links
    site_name = "metacritic.com"
    links = [(site_name + i["href"]) for i in album_name_text]





    # Scraping each album page - Yair's code
    # Iterate over every element in links





    table_dict = ({"Album": album_names,
                   "Artist": artist_names,
                   "Metascore": metascores,
                   "User Score": userscores,
                   "Release Date": release_dates,
                   "Summary": summaries,
                   "Link to Album Page": links})
    top_albums = pd.DataFrame(table_dict)
    top_albums.to_csv("top albums.csv")
    print(top_albums)


scrape(url)
