from spotipy.oauth2 import SpotifyOAuth
from spotipy import Spotify
import streamlit as st
import pandas as pd
import time
import requests
import json
import datetime
from google.cloud import storage
from google.oauth2 import service_account

# environment setup
REDIRECT_URI = 'https://spotify-divide.streamlit.app/'
MONTHLY_LIMIT = 5000
TRACK_CACHE_FOLDER = "track_cache/"

# streamlit secrets
RAPIDAPI_KEY = st.secrets["rapidapi"]["key"]
GCP_CREDS = st.secrets["gcp"]
SPOTIFY_CLIENT_ID = st.secrets["spotify"]["SPOTIFY_CLIENT_ID"]
SPOTIFY_CLIENT_SECRET = st.secrets["spotify"]["SPOTIFY_CLIENT_SECRET"]

# Bucket Names
GCS_BUCKET_NAME = "spotify-audio-features"
usage_bucket_name = "spotify-rapidapi-tracker"

# GCS Setup
credentials = service_account.Credentials.from_service_account_info(GCP_CREDS)
client = storage.Client(credentials=credentials)
bucket = client.bucket(GCS_BUCKET_NAME)
usage_bucket = client.bucket(usage_bucket_name)

# create and initialize tracking JSON if it does not exist
usage_blob = usage_bucket.blob("api_usage/rapidapi.json")  # path inside the bucket
if not usage_blob.exists():
    data = {
        "month": datetime.datetime.now().strftime("%Y-%m"),
        "calls_made": 0
    }
    usage_blob.upload_from_string(json.dumps(data), content_type="application/json")

# Oauth setup
oauth = SpotifyOAuth(
    client_id=SPOTIFY_CLIENT_ID,
    client_secret=SPOTIFY_CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
    scope='user-top-read',
    cache_path=None
)

# for spotify login
if "token_info" not in st.session_state:
    st.session_state.token_info = None

# get authorization code from URL if redirected
code = st.experimental_get_query_params().get("code")
if code and not st.session_state.token_info:
    st.session_state.token_info = oauth.get_access_token(code[0], as_dict=False)
    st.experimental_set_query_params()  # clear code from URL

# if not logged in, show login link and stop
if not st.session_state.token_info:
    auth_url = oauth.get_authorize_url()
    st.markdown(f"[Click here to log in with Spotify]({auth_url})")
    st.stop()  # Stop execution until user logs in

spotify = Spotify(auth=st.session_state.token_info["access_token"])
st.write("✅ Logged in as:", spotify.current_user()["display_name"])


# gets audio features for one song, and uploads it to GCS
def get_audio_features_by_spotify_id(track_id):
    # Load current usage
    usage_data = json.loads(usage_blob.download_as_text())
    current_month = datetime.datetime.now().strftime("%Y-%m")

    if usage_data["month"] != current_month:
        # Reset monthly usage at the start of a new month
        usage_data = {"month": current_month, "calls_made": 0}

    if usage_data["calls_made"] >= MONTHLY_LIMIT:
        st.error(f"RapidAPI monthly limit of {MONTHLY_LIMIT} reached. Using cached data only.")
        return None

    url = f"https://track-analysis.p.rapidapi.com/pktx/spotify/{track_id}"

    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "track-analysis.p.rapidapi.com"
    }

    time.sleep(1)  # wait one second before another rapid api call
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        # Update usage
        usage_data["calls_made"] += 1
        usage_blob.upload_from_string(json.dumps(usage_data), content_type="application/json")
        return response.json()

    except requests.exceptions.RequestException as e:
        st.error(f"RapidAPI error for {track_id}: {e}")
        return None

# Normalizes features for ease of use later
def normalize_features(api_data):
    minutes, seconds = api_data["duration"].split(":")
    duration_seconds = int(minutes) * 60 + int(seconds)
    loudness_db = int(api_data["loudness"].replace(" dB", ""))

    return {
        "key": api_data["key"],
        "mode": api_data["mode"],
        "camelot": api_data["camelot"],
        "tempo": api_data["tempo"],
        "duration_seconds": duration_seconds,
        "popularity": api_data["popularity"],
        "energy": api_data["energy"],
        "danceability": api_data["danceability"],
        "happiness": api_data["happiness"],
        "acousticness": api_data["acousticness"],
        "instrumentalness": api_data["instrumentalness"],
        "liveness": api_data["liveness"],
        "speechiness": api_data["speechiness"],
        "loudness_db": loudness_db
    }


# streamlit UI
st.set_page_config(page_title='Spotify Data Harvesting!', page_icon=':musical_note')
st.title('I am downloading your data')
st.write('All your songs are belong to me')

# get top n tracks from user
n = 25
top_tracks = spotify.current_user_top_tracks(limit=n, time_range='short_term')['items']

# display top tracks on streamlit
if st.button("Show Top Tracks"):
    # for loop goes around top n tracks
    for i in range(n):
        track = top_tracks[i]
        st.write(f"{track['name']} By: {track['artists'][0]['name']}")

        # Define the blob name in the bucket
        track_id = track["uri"].split(":")[-1]
        blob_name = f"tracks/{track_id}.json"
        blob = bucket.blob(blob_name)

        # logic to not duplicate blobs, blob name is track id
        if blob.exists():
            st.write("Already processed, skipping")
            continue

        st.caption("Analyzing track...")

        # call RAPID API Track Analysis
        analysis = get_audio_features_by_spotify_id(track_id)
        if analysis:
            features = normalize_features(analysis)
            payload = {
                "track_id": track_id,
                "name": track["name"],
                "artist": track["artists"][0]["name"],
                "uri": track["uri"],
                "audio_features": features,
                "source": "rapidapi",
                "analysis_version": "v1"
            }
            blob.upload_from_string(json.dumps(payload), content_type="application/json")

    st.success("Top 10 tracks uploaded successfully!")
