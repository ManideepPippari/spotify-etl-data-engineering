import os
import io
import json
import logging
from datetime import datetime
import csv

import boto3
import requests

# ---------- LOGGING ----------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------- ENVIRONMENT VARIABLES ----------
# S3
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
S3_PREFIX = os.environ.get("S3_PREFIX", "spotify/processed/")

# Spotify credentials
SPOTIFY_CLIENT_ID = os.environ["SPOTIFY_CLIENT_ID"]
SPOTIFY_CLIENT_SECRET = os.environ["SPOTIFY_CLIENT_SECRET"]

# Artist IDs (comma-separated string in env var)
ARTIST_IDS_RAW = os.environ.get("ARTIST_IDS", "")
ARTIST_IDS = [a.strip() for a in ARTIST_IDS_RAW.split(",") if a.strip()]
if not ARTIST_IDS:
    raise ValueError(
        "No ARTIST_IDS provided. Set ARTIST_IDS env var (comma-separated artist IDs)."
    )

s3_client = boto3.client("s3")


# ---------- SPOTIFY AUTH ----------
def get_spotify_token() -> str:
    token_url = "https://accounts.spotify.com/api/token"
    payload = {"grant_type": "client_credentials"}
    auth = (SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)

    resp = requests.post(token_url, data=payload, auth=auth)

    if not resp.ok:
        logger.error(
            "Failed to get Spotify token. "
            f"Status: {resp.status_code}, Body: {resp.text}"
        )
        resp.raise_for_status()

    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError(
            f"Spotify token response missing access_token: {json.dumps(data)}"
        )

    return token


# ---------- SPOTIFY DATA FETCH ----------
def get_artist_top_tracks(artist_id: str, token: str):
    url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"market": "US"}

    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()

    return resp.json().get("tracks", [])


def fetch_rows(token: str):
    """Return list[dict] of all tracks for all artists."""
    all_rows = []

    for artist_id in ARTIST_IDS:
        tracks = get_artist_top_tracks(artist_id, token)
        logger.info(f"Artist {artist_id} - fetched {len(tracks)} tracks")

        for t in tracks:
            row = {
                "artist": ", ".join([a["name"] for a in t.get("artists", [])]),
                "album_name": t.get("album", {}).get("name"),
                "track_name": t.get("name"),
                "track_id": t.get("id"),
                "duration_ms": t.get("duration_ms"),
                "explicit": t.get("explicit"),
                "album_release_date": t.get("album", {}).get("release_date"),
                "track_popularity": t.get("popularity"),
                "album_id": t.get("album", {}).get("id"),
            }
            all_rows.append(row)

    return all_rows


# ---------- TRANSFORM (PURE PYTHON) ----------
def transform_rows(rows):
    """Apply the same logic we had in pandas, but using plain Python."""

    # 1) Drop rows without track_id and remove duplicates
    cleaned = []
    seen_track_ids = set()

    for r in rows:
        tid = r.get("track_id")
        if not tid:
            continue
        if tid in seen_track_ids:
            continue
        seen_track_ids.add(tid)
        cleaned.append(dict(r))  # shallow copy

    # 2) duration_minutes and length_category
    for r in cleaned:
        ms = r.get("duration_ms")
        if isinstance(ms, (int, float)):
            minutes = round(ms / 1000.0 / 60.0, 2)
        else:
            minutes = None
        r["duration_minutes"] = minutes

        if minutes is None:
            length_cat = None
        elif minutes < 3:
            length_cat = "Short (<3 min)"
        elif 3 <= minutes <= 5:
            length_cat = "Medium (3-5 min)"
        else:
            length_cat = "Long (>5 min)"

        r["length_category"] = length_cat

    # 3) album_track_count
    album_counts = {}
    for r in cleaned:
        album = r.get("album_name")
        if not album:
            continue
        album_counts[album] = album_counts.get(album, 0) + 1

    # 4) album_popularity_rank (dense rank, higher count = rank 1)
    sorted_albums = sorted(album_counts.items(), key=lambda x: x[1], reverse=True)
    album_rank = {}
    current_rank = 0
    previous_count = None

    for album, count in sorted_albums:
        if count != previous_count:
            current_rank += 1
            previous_count = count
        album_rank[album] = current_rank

    # 5) Attach counts & ranks back to each row
    for r in cleaned:
        album = r.get("album_name")
        r["album_track_count"] = album_counts.get(album)
        r["album_popularity_rank"] = album_rank.get(album)

    return cleaned


# ---------- S3 UPLOAD ----------
def upload_to_s3(rows):
    now_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    key = f"{S3_PREFIX}tracks_transformed_{now_str}.csv"

    csv_buffer = io.StringIO()

    if rows:
        fieldnames = list(rows[0].keys())
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    else:
        # still create a file with just headers for consistency
        writer = csv.writer(csv_buffer)
        writer.writerow(
            [
                "artist",
                "album_name",
                "track_name",
                "track_id",
                "duration_ms",
                "explicit",
                "album_release_date",
                "track_popularity",
                "album_id",
                "duration_minutes",
                "length_category",
                "album_track_count",
                "album_popularity_rank",
            ]
        )

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=key,
        Body=csv_buffer.getvalue().encode("utf-8"),
    )

    logger.info(f"Uploaded transformed data to s3://{S3_BUCKET_NAME}/{key}")
    return key


# ---------- LAMBDA HANDLER ----------
def lambda_handler(event, context):
    try:
        logger.info("Starting Spotify ETL Lambda")

        token = get_spotify_token()
        raw_rows = fetch_rows(token)
        logger.info(f"Raw rows fetched: {len(raw_rows)}")

        transformed_rows = transform_rows(raw_rows)
        logger.info(f"Transformed rows: {len(transformed_rows)}")

        s3_key = upload_to_s3(transformed_rows)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Spotify ETL completed successfully",
                    "row_count": int(len(transformed_rows)),
                    "s3_key": s3_key,
                }
            ),
        }

    except Exception as e:
        logger.exception("Error in Spotify ETL Lambda")
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "Error in Spotify ETL Lambda",
                    "error": str(e),
                }
            ),
        }