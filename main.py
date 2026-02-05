import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy import Spotify
import streamlit as st
import os
import pandas as pd
import time
import requests
import json
import datetime
from google.cloud import storage
from google.oauth2 import service_account

# environment setup

MONTHLY_LIMIT = 5000
TRACK_CACHE_FOLDER = "track_cache/"

# streamlit secrets
RAPIDAPI_KEY = st.secrets["rapidapi"]["key"]
GCP_CREDS = st.secrets["gcp"]


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



# spotify helper functions
def get_token(oauth, code):
    token = oauth.get_access_token(code, as_dict=False, check_cache=False)
    # return the token
    return token

def sign_in(token):
    sp = spotipy.Spotify(auth=token)
    return sp

def app_get_token():
    try:
        token = get_token(st.session_state["oauth"], st.session_state["code"])
    except Exception as e:
        st.error("An error has occured during token retreval!")
        st.write("The error is as follows")
        st.write(e)
    else:
        st.session_state["cached_token"] = token

def app_sign_in():
    try:
        sp = sign_in(st.session_state["cached_token"])
    except Exception as e:
        st.error("An error occurred during sign-in!")
        st.write("The error is as follows:")
        st.write(e)
    else:
        st.session_state["signed_in"] = True
        app_display_welcome()
        st.success("Sign in success!")

    return sp

def app_display_welcome():
    oauth = SpotifyOAuth(
        client_id=st.secrets["spotify"]["SPOTIFY_CLIENT_ID"],
        client_secret=st.secrets["spotify"]["SPOTIFY_CLIENT_SECRET"],
        redirect_uri=st.secrets["spotify"]["SPOTIFY_REDIRECT_URI"],
        scope='user-top-read',
        cache_handler=None
    )

    # store oauth in session
    st.session_state["oauth"] = oauth

    # retrieve auth url
    auth_url = oauth.get_authorize_url()

    # should open link in the same tab when streamlit cloud is updated via _self target
    link_html = " <a target=\"_self\" href=\"{url}\" >{msg}</a> ".format(
        url=auth_url,
        msg="Click me to authenticate!"
    )

    welcome_msg = """
        Welcome! :wave: This app uses the Spotify API to interact with general 
        music info! In order to upload data associated with your account, you
        must log in. You only need to do this once.
        """
    note_temp = """
        _Note: Unfortunately, the current version of Streamlit will not allow for
        staying on the same page, so the authorization and redirection will open in a 
        new tab. This has already been addressed in a development release, so it should
        be implemented in Streamlit Cloud soon!_
        """

    if not st.session_state["signed_in"]:
        st.markdown(welcome_msg)
        st.write(" ".join(["No tokens found for this session. Please log in by",
                           "clicking the link below."]))
        st.markdown(link_html, unsafe_allow_html=True)
        st.markdown(note_temp)

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

# app session variable initialization
if "signed_in" not in st.session_state:
    st.session_state["signed_in"] = False
if "cached_token" not in st.session_state:
    st.session_state["cached_token"] = ""
if "code" not in st.session_state:
    st.session_state["code"] = ""
if "oauth" not in st.session_state:
    st.session_state["oauth"] = None
if "token_info" not in st.session_state:
    st.session_state.token_info = None
# makes sure not to reuse old code
st.session_state.pop("code", None)
st.session_state.pop("token", None)

# oauth object moved up to before get_token() call
if "oauth" not in st.session_state or st.session_state["oauth"] is None:
    # import secrets from streamlit
    SPOTIFY_CLIENT_ID = st.secrets["spotify"]["SPOTIFY_CLIENT_ID"]
    SPOTIFY_CLIENT_SECRET = st.secrets["spotify"]["SPOTIFY_CLIENT_SECRET"]
    REDIRECT_URI = st.secrets["spotify"]["SPOTIFY_REDIRECT_URI"]

    # Oauth setup
    st.session_state["oauth"] = SpotifyOAuth(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope='user-top-read',
        cache_handler=None
    )
url_params = st.query_params

# attempt to sign in with cached token
if st.session_state["cached_token"] != "":
    sp = app_sign_in()
# if no token, but code in url, get code, parse token, then sign in
elif "code" in url_params:
    # all params stored as lists
    st.session_state["code"] = url_params["code"][0]
    st.write(url_params["code"])

    app_get_token()
    sp = app_sign_in()
# prompt for redirect
else:
    app_display_welcome()

if st.session_state["signed_in"]:
    user = sp.current_user()
    name = user["display_name"]
    username = user["id"]


    st.write("✅ Logged in as:", user)

    # streamlit UI
    st.set_page_config(page_title='Spotify Data Harvesting!', page_icon=':musical_note')
    st.title('I am downloading your data')
    st.write('All your songs are belong to me')

    # get top n tracks from user
    n = 25
    top_tracks = sp.current_user_top_tracks(limit=n, time_range='short_term')['items']

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
