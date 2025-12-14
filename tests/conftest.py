"""
Shared pytest fixtures for Spotify ETL tests.

These fixtures provide small, realistic samples that mirror the
actual shapes used in the project, without overfitting to exact
production numbers.

The goal is to support unit tests, not to fully simulate Spotify,
AWS, or Snowflake.
"""

import pytest
import pandas as pd
from datetime import datetime


# -------------------------------------------------------------------
# Environment fixtures
# -------------------------------------------------------------------

@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    """Set required environment variables for tests"""
    monkeypatch.setenv("SPOTIFY_CLIENT_ID", "test_client_id")
    monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "test_client_secret")
    monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")
    monkeypatch.setenv("S3_PREFIX", "spotify/processed/")
    monkeypatch.setenv("ARTIST_IDS", "artist1,artist2")


# -------------------------------------------------------------------
# Spotify API fixtures
# -------------------------------------------------------------------

@pytest.fixture
def spotify_token_response():
    """Minimal mock OAuth token response"""
    return {
        "access_token": "test_access_token",
        "token_type": "Bearer",
        "expires_in": 3600
    }


@pytest.fixture
def spotify_tracks_response():
    """Simplified Spotify tracks payload"""
    return {
        "tracks": [
            {
                "id": "track_1",
                "name": "Sample Song",
                "duration_ms": 200000,
                "explicit": False,
                "popularity": 80,
                "artists": [{"name": "Sample Artist"}],
                "album": {
                    "id": "album_1",
                    "name": "Sample Album",
                    "release_date": "2020-01-01"
                }
            }
        ]
    }


# -------------------------------------------------------------------
# CSV / DataFrame fixtures
# -------------------------------------------------------------------

@pytest.fixture
def raw_tracks_csv():
    """Raw CSV sample before transformation"""
    return (
        "artist,album_name,track_name,track_id,duration_ms,explicit\n"
        "Sample Artist,Sample Album,Sample Song,track_1,200000,False\n"
    )


@pytest.fixture
def transformed_tracks_df():
    """Expected DataFrame after transformation"""
    return pd.DataFrame(
        {
            "artist": ["Sample Artist"],
            "album_name": ["Sample Album"],
            "track_name": ["Sample Song"],
            "track_id": ["track_1"],
            "duration_ms": [200000],
            "explicit": [False],
            "duration_minutes": [3.33],
            "length_category": ["Medium (3-5 min)"],
        }
    )


# -------------------------------------------------------------------
# S3 event fixture
# -------------------------------------------------------------------

@pytest.fixture
def s3_put_event():
    """Minimal S3 PUT event for Lambda trigger"""
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": "spotify/processed/test.csv"}
                }
            }
        ]
    }


# -------------------------------------------------------------------
# Airflow context fixture
# -------------------------------------------------------------------

@pytest.fixture
def airflow_context():
    """Mock Airflow task context"""
    from unittest.mock import Mock

    ti = Mock()
    ti.xcom_pull.return_value = "/tmp/test.csv"

    return {
        "ti": ti,
        "execution_date": datetime.utcnow()
    }
