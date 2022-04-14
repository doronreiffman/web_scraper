"""
Calls Spotify API for additional attributes
"""

import config as cfg
import requests
from urllib.parse import urlencode
import logging


def spotify_search(query, search_type):
    """
    Searches Spotify with given query and search_type using Spotify's API, returns json with results
    :param query: a string of the free text search
    :param search_type:# a string of the search type
    :return: the response from the website
    """

    # query is required according to Spotify API
    if query is None:
        raise Exception("A query is required")

    # Spotify API requires search_type to be one of these types
    allowable_search_types = ["album", "artist", "playlist", "track", "show", "episode"]
    if search_type not in allowable_search_types:
        raise Exception(f"The search type must be one of : {allowable_search_types}")

    # save parameters as url to search
    endpoint = f"{cfg.BASE_URL}/search"
    search_params = urlencode({"q": query, "type": search_type.lower()})
    lookup_url = f"{endpoint}?{search_params}"

    try:
        r = requests.get(lookup_url, headers=cfg.HEADERS)
    except Exception as e:
        logging.warning(f"Search for {query} was unsuccessful. \n {e}")
        raise Exception(f"Search for {query} was unsuccessful. \n {e}.")

    # check that our search was successful
    if r.status_code not in range(200, 299):
        logging.warning(f"Could not complete search for {query}.")
        raise Exception(f"Could not complete search for {query}.")
    return r.json()
