# src/ingestion/extract_local.py

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd

from src.config import (
    SPOTIFY_CLIENT_ID,
    SPOTIFY_CLIENT_SECRET,
)

def get_spotify_client():
    """
    Spotify client using Client Credentials flow.
    This does NOT open a browser – perfect for Airflow/Docker.
    """
    auth_manager = SpotifyClientCredentials(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
    )
    return spotipy.Spotify(auth_manager=auth_manager)


def extract():
    """
    Extract tracks for multiple artists from Spotify and return a pandas DataFrame.
    """

    sp = get_spotify_client()
    tracks_data = []

    # 🔹 Five artists
    artist_ids = [
        "1Xyo4u8uXC1ZmMpatF05PJ",  # The Weeknd
        "06HL4z0CvFAxyc27GXpf02",  # Taylor Swift
        "3TVXtAsR1Inumwj472S9r4",  # Drake
        "6eUKZXaKkcviH0Ku9w2n3V",  # Ed Sheeran
        "6qqNVTkY8uBg9cP3Jd7DAH",  # Billie Eilish
    ]

    for artist_id in artist_ids:
        artist_info = sp.artist(artist_id)
        artist_name = artist_info["name"]

        albums = sp.artist_albums(artist_id, limit=20)

        for album in albums["items"]:
            album_id = album["id"]
            album_name = album["name"]

            tracks = sp.album_tracks(album_id)

            for track in tracks["items"]:
                tracks_data.append(
                    {
                        "artist": artist_name,
                        "artist_id": artist_id,
                        "album_name": album_name,
                        "album_id": album_id,
                        "track_name": track["name"],
                        "track_id": track["id"],
                        "duration_ms": track["duration_ms"],
                        "explicit": track["explicit"],
                    }
                )

    df = pd.DataFrame(tracks_data)
    print(df["artist"].value_counts())
    return df


if __name__ == "__main__":
    df = extract()
    print(df.head())
    df.to_csv("tracks_raw.csv", index=False)