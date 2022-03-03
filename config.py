# Logging configuration
LOGFILE_NAME = "logfile.log"
LOGFILE_DEBUG = True  # When the flag is true the logging level is debug

# URL configuration
SITE_ADDRESS = "https://www.metacritic.com"
URL_INDEX = 0  # Choose by URL_INDEX the wanted chart from the list URL_LIST.
URL_LIST = ["https://www.metacritic.com/browse/albums/score/metascore/all?sort=desc",
            "https://www.metacritic.com/browse/albums/score/metascore/year/filtered?sort=desc",
            "https://www.metacritic.com/browse/albums/score/metascore/discussed/filtered?sort=desc&view=detailed"]

#  requests status configuration
REQ_STATUS_LOWER = 200
REQ_STATUS_UPPER = 299

#  Scraping configurations
METASCORE_INC = 2

# strings to strip from longer strings of text
STRIP_BEG = "\n by "
STRIP_END = "\n "
