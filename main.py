import spotipy
from spotipy.oauth2 import SpotifyOAuth
import streamlit as st
import pandas as pd
import requests
import json
from google.cloud import storage
from google.oauth2 import service_account

# environment setup
REDIRECT_URI = 'http://127.0.0.1:9090'
RAPIDAPI_KEY = st.secrets["rapidapi"]

# Oauth setup
sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=st.secrets["spotify"]["SPOTIFY_CLIENT_ID"],
        client_secret=st.secrets["spotify"]["SPOTIFY_CLIENT_SECRET"],
        redirect_uri=REDIRECT_URI,
        scope='user-top-read'
    )
)

# Get the path to your service account JSON
GCS_BUCKET_NAME = "spotify-audio-features"

if not GCS_BUCKET_NAME:
    st.error("GCS credentials or bucket name not set!")
    st.stop()

# Create a storage client using st.secrets
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp"])
client = storage.Client(credentials=credentials)
bucket = client.bucket(GCS_BUCKET_NAME)

# gets audio features for one song, need to add way to check server first, so I don't call the api too much
def get_audio_features_by_spotify_id(track_id):
    url = f"https://track-analysis.p.rapidapi.com/pktx/spotify/{track_id}"

    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "track-analysis.p.rapidapi.com"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        st.error(f"RapidAPI error for {track_id}: {e}")
        return None

    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:
            # Specific handling for “rate limit reached”
            st.error(f"Rate limit reached for track {track_id}. Try again tomorrow or check cached results.")
        else:
            st.error(f"RapidAPI HTTP error for {track_id}: {e}")
        return None

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

# streamlit setup
st.set_page_config(page_title='Spotify Two Halfs', page_icon=':musical_note')
st.title('Title Test')
st.write('This is the page, hopefully it all works')

# get top n tracks from user
n = 5
top_tracks = sp.current_user_top_tracks(limit=n, time_range='short_term')['items']

# display top tracks on streamlit
if st.button("Show Top Tracks"):
    #for loop goes around top n tracks
    for i in range(n):
        st.write(
            f"{top_tracks[i]['name']} By: "
            f"{top_tracks[i]['artists'][0]['name']}"
        )

        # Define the blob name in the bucket
        track_id = top_tracks[i]["uri"].split(":")[-1]
        blob_name = f"tracks/{track_id}.json"

        # Create the blob and upload the JSON
        blob = bucket.blob(blob_name)
        #logic to not duplicate blobs
        if blob.exists():
            st.write("Already processed, skipping")
            continue

        st.caption("Analyzing track...")
        #call RAPID API Track Analysis
        analysis = get_audio_features_by_spotify_id(track_id)
        features = normalize_features(analysis)

        #format data for upload
        payload = {
            "track_id": track_id,
            "name": top_tracks[i]['name'],
            "artist": top_tracks[i]["artists"][0]["name"],
            "uri": top_tracks[i]["uri"],
            "audio_features": features,
            "source": "rapidapi",
            "analysis_version": "v1"
        }

        blob.upload_from_string(
            json.dumps(payload),
            content_type="application/json"
        )

    st.success("Top 10 tracks uploaded successfully!")
