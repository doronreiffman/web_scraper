from bs4 import BeautifulSoup
import requests
import pandas as pd

main_link = 'https://www.metacritic.com'

source = requests.get('https://www.metacritic.com/music/fetch-the-bolt-cutters/fiona-apple',
                      headers={'User-Agent': 'Mozilla/5.0'}).text

#source = requests.get('https://www.metacritic.com/music/dont-tread-on-me/311',
 #                     headers={'User-Agent': 'Mozilla/5.0'}).text

soup = BeautifulSoup(source, 'lxml')

# Title (Album)
title_name = soup.find('div', class_='product_title').a.span.h1.text

# Artist
artist_html = soup.find('div', class_='product_artist')
artist_link = main_link + artist_html.a['href']
artist_name = artist_html.a.span.text

# Publisher
publisher_html = soup.find('span', class_='data', itemprop='publisher')
publisher_link = main_link + publisher_html.a['href']
publisher_name = publisher_html.a.span.text.strip()

# Release Date
release_date = soup.find('span', class_='data', itemprop='datePublished').text

# Album Cover
album_cover_image = soup.find('img', class_='product_image large_image')['src']

# Genres
genres = soup.find('li', class_='summary_detail product_genre')
genres_list = '\n'.join([genre.text for genre in genres.findAll('span')])

# Album Description
album_description = soup.find('span', itemprop="description").text

# Link to Buy the Album
buy_album_link = soup.find('td', class_="esite_img_wrapper")
if buy_album_link:
    buy_album_link = buy_album_link.a["href"]
else:
    buy_album_link = ''

# Metascore
meta_score = soup.find('span', itemprop="ratingValue").text
meta_score_count = soup.find('span', itemprop="reviewCount").text.strip()
meta_score_link = main_link + soup.find('li', class_="nav nav_critic_reviews").span.span.a["href"]
# meta_score_link = meta_score_link.span.span.a.text


# Userscore
user_score_html = soup.find('div', class_="userscore_wrap feature_userscore")
user_score = user_score_html.find('a', class_='metascore_anchor').div.text
user_score_count = user_score_html.find('span', class_='count').a.text
user_score_link = soup.find('li', class_='nav nav_user_reviews').span.span.a["href"]

# details and credits
details_credits_link = main_link + soup.find('li', class_="nav nav_details last_nav").span.span.a["href"]

a_dict = {}
var_list = ['title_name', 'artist_link', 'artist_name', 'publisher_link', 'genres_list', 'details_credits_link',
            'user_score_link', 'user_score', 'user_score_count', 'meta_score', 'meta_score_count', 'meta_score_link',
            'publisher_name', 'release_date', 'album_cover_image', 'album_description', 'buy_album_link']

for n, variable in enumerate(var_list):
    a_dict[variable] = [eval(variable)]

df = pd.DataFrame(data=a_dict)
df.to_csv("data.csv")
