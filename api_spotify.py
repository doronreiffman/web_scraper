import requests
from urllib.parse import urlencode

# application link: https://developer.spotify.com/dashboard/applications/fb71510e7a1e42b3b9f23045abba5d39

# TODO: add to separate file?
CLIENT_ID = 'fb71510e7a1e42b3b9f23045abba5d39'
CLIENT_SECRET = '271c34473cfc42b993bde54c906b3c41'

AUTH_URL = 'https://accounts.spotify.com/api/token'
BASE_URL = 'https://api.spotify.com/v1'
AUTH_RESPONSE = requests.post(AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
})
# convert the response to JSON
AUTH_RESPONSE_DATA = AUTH_RESPONSE.json()

# save the access token
ACCESS_TOKEN = AUTH_RESPONSE_DATA['access_token']

headers = {
    'Authorization': 'Bearer {token}'.format(token=ACCESS_TOKEN)
}


# TODO: move to tests
def perform_auth():
    if CLIENT_ID is None or CLIENT_SECRET is None:
        raise Exception("You must set client_id and client_secret")
    r = requests.post(AUTH_URL, data=AUTH_RESPONSE_DATA, headers=headers)
    if r.status_code not in range(200, 299):
        raise Exception("Could not authenticate client.")
    return True


def spotify_search(query, search_type):  # type
    if query is None:
        raise Exception("A query is required")
    endpoint = f"{BASE_URL}/search"
    search_params = urlencode({"q": query, "type": search_type.lower()})
    lookup_url = f"{endpoint}?{search_params}"
    r = requests.get(lookup_url, headers=headers)
    if r.status_code not in range(200, 299):
        raise Exception("Could not complete search.")
    print(f"Calling Spotify API, searching for {query} in {search_type}s")
    return r.json()
