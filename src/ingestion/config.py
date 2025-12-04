import os

"""
Config for ingestion package (Spotify API → local / S3).

⚠ No real secrets are stored in this file.
All sensitive values must come from environment variables.
"""

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "CHANGE_ME_IN_ENV")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "CHANGE_ME_IN_ENV")
SPOTIFY_REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI", "http://127.0.0.1:9090/callback")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "mani-spotify-etl-data")
S3_PROCESSED_PREFIX = os.getenv("S3_PROCESSED_PREFIX", "spotify/processed")