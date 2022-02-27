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
# https://www.metacritic.com/browse/albums/score/metascore/all/filtered?sort=desc

metacritic_scraper.scraper(https://www.metacritic.com/browse/albums/score/metascore/all/filtered?sort=desc)
```


## Authors

- [@YairVag](https://www.github.com/YairVag)
- [@doronreiffman](https://www.github.com/doronreiffman)

