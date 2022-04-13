"""
Calls Spotify API for additional attributes
"""

import config as cfg
import requests
from urllib.parse import urlencode


# application link: https://developer.spotify.com/dashboard/applications/fb71510e7a1e42b3b9f23045abba5d39

# TODO: move to tests
def perform_auth():
    """
    checks validity of access token
    """
    if cfg.CLIENT_ID is None or cfg.CLIENT_SECRET is None:
        raise Exception("You must set client_id and client_secret")
    r = requests.post(cfg.AUTH_URL, data=cfg.AUTH_RESPONSE_DATA, headers=cfg.HEADERS)
    if r.status_code not in range(200, 299):
        raise Exception("Could not authenticate client.")
    return True


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
    r = requests.get(lookup_url, headers=cfg.HEADERS)

    # check that our search was successful
    if r.status_code not in range(200, 299):
        raise Exception("Could not complete search.")
    return r.json()
