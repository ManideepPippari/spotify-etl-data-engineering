"""
Unit tests for AWS Lambda ingestion: lambda/spotify_lambda_ingest.py

Focus:
- Token retrieval (Spotify OAuth)
- Fetch + transform happy path
- S3 upload call (put_object)
- lambda_handler success + error response

We keep tests small and deterministic by mocking requests + S3 client.
"""

import json
import sys
from unittest.mock import Mock, patch

import pytest

# allow importing lambda module from repo root
sys.path.insert(0, "./lambda")

from spotify_lambda_ingest import (  # noqa: E402
    get_spotify_token,
    fetch_rows,
    transform_rows,
    upload_to_s3,
    lambda_handler,
)


# -------------------------
# Fixtures
# -------------------------

@pytest.fixture
def token_payload():
    return {"access_token": "test_token", "token_type": "Bearer", "expires_in": 3600}


@pytest.fixture
def top_tracks_payload():
    return {
        "tracks": [
            {
                "id": "track_1",
                "name": "Song A",
                "duration_ms": 200000,
                "explicit": False,
                "popularity": 80,
                "artists": [{"name": "Artist A", "id": "artist_a"}],
                "album": {"name": "Album A", "id": "album_a", "release_date": "2020-01-01"},
            },
            {
                "id": "track_2",
                "name": "Song B",
                "duration_ms": 240000,
                "explicit": True,
                "popularity": 70,
                "artists": [{"name": "Artist B", "id": "artist_b"}],
                "album": {"name": "Album B", "id": "album_b", "release_date": "2021-01-01"},
            },
        ]
    }


# -------------------------
# Tests
# -------------------------

@patch("spotify_lambda_ingest.requests.post")
def test_get_spotify_token_success(mock_post, token_payload):
    mock_resp = Mock()
    mock_resp.ok = True
    mock_resp.json.return_value = token_payload
    mock_post.return_value = mock_resp

    token = get_spotify_token()
    assert token == "test_token"
    mock_post.assert_called_once()


@patch("spotify_lambda_ingest.requests.post")
def test_get_spotify_token_missing_access_token_raises(mock_post):
    mock_resp = Mock()
    mock_resp.ok = True
    mock_resp.json.return_value = {"error": "invalid_client"}
    mock_post.return_value = mock_resp

    with pytest.raises(RuntimeError):
        get_spotify_token()


@patch("spotify_lambda_ingest.get_artist_top_tracks")
def test_fetch_rows_multiple_artists(mock_get_tracks, monkeypatch):
    # simulate 2 artists, one track each
    mock_get_tracks.side_effect = [
        [
            {
                "id": "track_1",
                "name": "Song A",
                "duration_ms": 200000,
                "explicit": False,
                "popularity": 80,
                "artists": [{"name": "Artist A"}],
                "album": {"name": "Album A", "id": "alb1", "release_date": "2020-01-01"},
            }
        ],
        [
            {
                "id": "track_2",
                "name": "Song B",
                "duration_ms": 180000,
                "explicit": True,
                "popularity": 75,
                "artists": [{"name": "Artist B"}],
                "album": {"name": "Album B", "id": "alb2", "release_date": "2021-01-01"},
            }
        ],
    ]

    import spotify_lambda_ingest as mod
    original = mod.ARTIST_IDS
    mod.ARTIST_IDS = ["artist_a", "artist_b"]

    try:
        rows = fetch_rows("test_token")
    finally:
        mod.ARTIST_IDS = original

    assert len(rows) == 2
    assert rows[0]["track_id"] == "track_1"
    assert rows[1]["track_id"] == "track_2"


def test_transform_rows_adds_duration_and_length_category():
    raw_rows = [
        {
            "artist": "Artist A",
            "album_name": "Album A",
            "track_name": "Song A",
            "track_id": "track_1",
            "duration_ms": 120000,  # 2 min
            "explicit": False,
            "album_release_date": "2020-01-01",
            "track_popularity": 80,
            "album_id": "alb1",
        },
        {
            "artist": "Artist B",
            "album_name": "Album B",
            "track_name": "Song B",
            "track_id": "track_2",
            "duration_ms": 360000,  # 6 min
            "explicit": True,
            "album_release_date": "2021-01-01",
            "track_popularity": 70,
            "album_id": "alb2",
        },
    ]

    out = transform_rows(raw_rows)

    assert len(out) == 2
    assert out[0]["duration_minutes"] == 2.0
    assert out[0]["length_category"].startswith("Short")
    assert out[1]["duration_minutes"] == 6.0
    assert out[1]["length_category"].startswith("Long")


@patch("spotify_lambda_ingest.s3_client.put_object")
def test_upload_to_s3_calls_put_object(mock_put):
    mock_put.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}

    rows = [
        {
            "artist": "Artist",
            "album_name": "Album",
            "track_name": "Song",
            "track_id": "track_1",
            "duration_ms": 200000,
            "explicit": False,
            "album_release_date": "2020-01-01",
            "track_popularity": 80,
            "album_id": "alb1",
            "duration_minutes": 3.33,
            "length_category": "Medium (3-5 min)",
            "album_track_count": 1,
            "album_popularity_rank": 1,
        }
    ]

    key = upload_to_s3(rows)

    assert "spotify/processed/" in key
    assert key.endswith(".csv")
    mock_put.assert_called_once()


@patch("spotify_lambda_ingest.upload_to_s3")
@patch("spotify_lambda_ingest.transform_rows")
@patch("spotify_lambda_ingest.fetch_rows")
@patch("spotify_lambda_ingest.get_spotify_token")
def test_lambda_handler_success(mock_token, mock_fetch, mock_transform, mock_upload):
    mock_token.return_value = "test_token"
    mock_fetch.return_value = [{"track_id": "track_1"}]
    mock_transform.return_value = [{"track_id": "track_1"}]
    mock_upload.return_value = "spotify/processed/file.csv"

    resp = lambda_handler({}, None)

    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["row_count"] == 1
    assert "s3_key" in body


@patch("spotify_lambda_ingest.get_spotify_token")
def test_lambda_handler_returns_500_on_error(mock_token):
    mock_token.side_effect = Exception("auth failed")

    resp = lambda_handler({}, None)

    assert resp["statusCode"] == 500
    body = json.loads(resp["body"])
    assert "error" in body
