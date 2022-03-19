# Metacritic Album Charts Scraper

Metacritic is a website that that aggregates reviews of music, movies, TV shows, and games.

This program will take any Metacritic music album chart and scrape it for relevant information, including:

* Album name (and link to Metacritic album page)
* Artist name (and link to Metacritic artist page)
* Album release date
* Metascore (based on critics' reviews)
* User score (based on users' reviews)
* Album summary
* Publisher (and link to Metacritic publisher page)
* Album Genres
* Album cover
* Link to Amazon purchase page

## Example

```javascript
# Returns information about the top-rated albums of all time, 
# based on the following link: 
# https://www.metacritic.com/browse/albums/score/metascore/all/

```


## Run Locally

## Steps: 

### Clone the project

```bash
  git clone https://github.com/doronreiffman/web_scraper
```

### Go to the project directory

```bash
  cd web_scraper
```

### Install dependencies

```bash
  pip install -r requirements.txt
```

### Save MySQL login information


```bash
  python ./metacritic_scraper.py login --username your_username --password your_password
```
* **Note: The login information is saved in login.json file**

### Create top_albums database

```bash
  python ./metacritic_scraper.py settings -i
```

### Update top_albums database

1. Example of updating the database with the top albums in 2013 by meta_score (with long and short notation):
* **Note: These arguments are required:  --filter,  --year,  --sort**

```bash
  python ./metacritic_scraper.py update --filter year --year 2013 --sort meta_score
  python ./metacritic_scraper.py update -f year -y 2013 -s meta_score
```
2. Same example using batch of 10 urls in grequest and showing the progress (with long and short notation):

```bash
  python ./metacritic_scraper.py update --filter year --year 2013 --sort meta_score --batch 10 --url --progress
  python ./metacritic_scraper.py update -f year -y 2013 -s meta_score -b 10 -u -p
```

3. For more information about updating the database use the help flag:

```bash
python ./metacritic_scraper.py update -h   
usage: metacritic_scraper.py update [-h] -f FILTER -y YEAR -s SORT [-b BATCH] [-m MAX] [-p] [-u] [-S]

options:
  -h, --help                  show this help message and exit
  -f FILTER, --filter FILTER  Filter albums: ['all_time', '90_days', 'year', 'discussed', 'shared']
  -y YEAR, --year YEAR        Albums year release: 2010 to 2022
  -s SORT, --sort SORT        Sort albums: ['meta_score', 'user_score']
  -b BATCH, --batch BATCH     grequest batch size
  -m MAX, --max MAX           Maximum number of albums to scrape
  -p, --progress              Shows scraping progress
  -u, --url                   Shows scraped urls
  -S, --save                  Saves csv file with the data
```

### More Help

Use the following commands for more information:
```bash
python ./metacritic_scraper.py -h
usage: metacritic_scraper.py [-h] {login,settings,update} 

This program scrapes data from Metacritic's albums charts.
It stores the data in relational database.
It allows to filter the desire chart by criteria.

positional arguments:
  {login,settings,update}
    login               Update login information. "login -h" for more information
    settings            Change Settings. "settings -h" for more information
    update              Update database. "update -h" for more information
```
```bash
python ./metacritic_scraper.py login  -h
usage: metacritic_scraper.py login [-h] --username USERNAME --password PASSWORD

options:
  -h, --help           show this help message and exit
  --username USERNAME
  --password PASSWORD
```
```bash
python ./metacritic_scraper.py settings  -h
usage: metacritic_scraper.py settings [-h] [-i]

options:
  -h, --help  show this help message and exit
  -i, --init  Initiates the database
```
## ERD

![Output in Pycharm](https://user-images.githubusercontent.com/100131903/159134969-624aab99-3c38-4357-8aec-8d2f8c221e39.jpg
)
## Contributing

Contributions are always welcome!


## Authors

- [@YairVag](https://www.github.com/YairVag)
- [@doronreiffman](https://www.github.com/doronreiffman)

