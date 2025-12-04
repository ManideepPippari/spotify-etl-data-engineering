import os

"""
Central config for local Spotify ETL.

⚠ No real secrets are stored in code.
Set these via environment variables when running locally:

    SPOTIFY_CLIENT_ID
    SPOTIFY_CLIENT_SECRET
    SPOTIFY_REDIRECT_URI  (optional, has a default)
    S3_BUCKET_NAME
    S3_PROCESSED_PREFIX
"""

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "CHANGE_ME_IN_ENV")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "CHANGE_ME_IN_ENV")
SPOTIFY_REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI", "http://127.0.0.1:7777/callback")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "mani-spotify-etl-data")
S3_PROCESSED_PREFIX = os.getenv("S3_PROCESSED_PREFIX", "spotify/processed")