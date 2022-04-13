from datetime import date
import requests

# Logging configuration
LOGFILE_NAME = "logfile.log"
LOGFILE_DEBUG = False  # When the flag is true the logging level is debug

# URL configuration
SITE_ADDRESS = "https://www.metacritic.com"
FILTER_BY = {'all_time': 'all/', '90_days': '90day/', 'year': 'year/', 'discussed': 'discussed/',
             'shared': 'shared/'}
SORT_BY = {'meta_score': '/browse/albums/score/metascore/', 'user_score': '/browse/albums/score/userscore/'}
YEAR_RELEASE = {str(year): 'filtered?year_selected=' + str(year) for year in range(2010, date.today().year + 1)}
DATA_FOLDER = 'data/'

#  requests status configuration
REQ_STATUS_LOWER = 200
REQ_STATUS_UPPER = 299

#  Scraping configurations
SCORE_INC = 2

# strings to strip from longer strings of text
STRIP_BEG = "\n by "
STRIP_END = "\n "

# for tests
ARGS_4_TESTS = ['update', '-f', 'year', '-y', '2022', '-s', 'meta_score']

TEST_PAGES = [
    'https://www.metacritic.com/music/my-beautiful-dark-twisted-fantasy/kanye-west',
    'https://www.metacritic.com/music/the-archandroid/janelle-monae',
    'https://www.metacritic.com/music/the-guitar-song/jamey-johnson',
    'https://www.metacritic.com/music/sir-lucious-left-foot-the-son-of-chico-dusty/big-boi',
    'https://www.metacritic.com/music/ali-toumani/ali-farka-toure-and-toumani-diabate',
    'https://www.metacritic.com/music/the-witmark-demos-1962-1964/bob-dylan',
    'https://www.metacritic.com/music/black-tambourine/black-tambourine',
    'https://www.metacritic.com/music/silent-movies/marc-ribot',
    'https://www.metacritic.com/music/airtights-revenge/bilal',
    'https://www.metacritic.com/music/the-suburbs/arcade-fire',
    'https://www.metacritic.com/music/body-talk/robyn',
    'https://www.metacritic.com/music/this-is-still-it/the-method-actors',
    'https://www.metacritic.com/music/how-i-got-over/the-roots',
    'https://www.metacritic.com/music/halcyon-digest/deerhunter',
    'https://www.metacritic.com/music/assume-crash-position',
    'https://www.metacritic.com/music/cosmogramma/flying-lotus',
    'https://www.metacritic.com/music/i-see-the-sign/sam-amidon',
    'https://www.metacritic.com/music/spiral-shadow/kylesa',
    'https://www.metacritic.com/music/the-budos-band-iii/the-budos-band',
    'https://www.metacritic.com/music/have-one-on-me/joanna-newsom',
    'https://www.metacritic.com/music/quarantine-the-past/pavement',
    'https://www.metacritic.com/music/high-violet/the-national',
    'https://www.metacritic.com/music/before-today/ariel-pinks-haunted-graffiti',
    'https://www.metacritic.com/music/apollo-kids/ghostface-killah',
    'https://www.metacritic.com/music/this-is-happening/lcd-soundsystem'
]

ALBUM_PAGE_COLUMNS = ['Link to Artist Page', 'Publisher', 'Link to Publisher Page', 'Album Cover Image', 'Album Genres',
                      'No. of Critic Reviews', 'Link to Critic Reviews', 'No. of User Reviews', 'Link to User Reviews',
                      'Link to More Details and Album Credits', 'Amazon Link']

CHART_PAGE_COLUMNS = ['Album', 'Album Rank', 'Artist', 'Metascore',
                      'User Score', 'Release Date', 'Summary', 'Link to Album Page']

# API Search Config

CLIENT_ID = 'fb71510e7a1e42b3b9f23045abba5d39'
CLIENT_SECRET = '271c34473cfc42b993bde54c906b3c41'

AUTH_URL = 'https://accounts.spotify.com/api/token'
BASE_URL = 'https://api.spotify.com/v1'
AUTH_RESPONSE = requests.post(AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET})

# convert the response to JSON
AUTH_RESPONSE_DATA = AUTH_RESPONSE.json()

# save the access token
ACCESS_TOKEN = AUTH_RESPONSE_DATA['access_token']

HEADERS = {'Authorization': 'Bearer {token}'.format(token=ACCESS_TOKEN)}
