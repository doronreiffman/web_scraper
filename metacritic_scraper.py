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
<<<<<<< HEAD
    site_name = "https://metacritic.com"
    links = [(site_name + i["href"]) for i in album_name_text]
=======
    links = [(SITE_ADDRESS + i["href"]) for i in album_name_text]
>>>>>>> 31ca876 (combine both functions in metacritic_scraper.py)

    summery = ({"Album": album_names,
                "Artist": artist_names,
                "Metascore": metascores,
                "User Score": userscores,
                "Release Date": release_dates,
                "Summary": summaries,
                "Link to Album Page": links})

    summery.update(scrape_album_page(links))

    top_albums = pd.DataFrame(summery)
    top_albums.to_csv("top albums.csv")
    print(top_albums)


def scrape_album_page(pages_url):
    result = {}
    for url in pages_url:

        # Getting page information
        source = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}).text
        soup = BeautifulSoup(source, 'html.parser')

        result.setdefault('artist_link', []).append(SITE_ADDRESS + soup.find('div', class_='product_artist').a['href'])

        publisher_html = soup.find('span', class_='data', itemprop='publisher')
        result.setdefault('publisher_link', []).append([SITE_ADDRESS + publisher_html.a['href']])
        result.setdefault('publisher_name', []).append(publisher_html.a.span.text.strip())

        result.setdefault('album_cover_image', []).append(publisher_html.a.span.text.strip())

        genres = soup.find('li', class_='summary_detail product_genre')
        result.setdefault('genres_list', []).append('\n'.join([genre.text for genre in genres.findAll('span')]))

        result.setdefault('meta_score_count', []).append(soup.find('span', itemprop="reviewCount").text.strip())
        result.setdefault('meta_score_link', []).append(
            SITE_ADDRESS + soup.find('li', class_="nav nav_critic_reviews").span.span.a["href"])

        result.setdefault('user_score_link', []).append(
            SITE_ADDRESS + soup.find('li', class_='nav nav_user_reviews').span.span.a["href"])

        try:
            user_score_html = soup.find('div', class_="userscore_wrap feature_userscore")
            result.setdefault('user_score_count', []).append(user_score_html.find('span', class_='count').a.text)
        except Exception:
            result.setdefault('user_score_count', []).append('')

        result.setdefault('details_credits_link', []).append(
            SITE_ADDRESS + soup.find('li', class_="nav nav_details last_nav").span.span.a["href"])

        # Link to Buy the Album
        buy_album_link = soup.find('td', class_="esite_img_wrapper")
        if buy_album_link:
            result.setdefault('buy_album_link', []).append(buy_album_link.a["href"])
        else:
            result.setdefault('buy_album_link', []).append('')

    return result


if __name__ == '__main__':
    scrape(URL)
