"""
Unit tests for local Spotify extraction logic.

These tests focus on verifying:
- a DataFrame is returned
- expected columns are present
- multiple albums / tracks are handled

External API calls are mocked to keep tests fast and deterministic.
"""

import pandas as pd
import pytest
from unittest.mock import Mock, patch

from src.ingestion.extract_local import extract


@pytest.fixture
def mock_spotify_client():
    """Minimal mocked Spotify client"""
    client = Mock()

    client.artist.return_value = {
        "id": "artist_1",
        "name": "Sample Artist"
    }

    client.artist_albums.return_value = {
        "items": [
            {"id": "album_1", "name": "Album One"},
            {"id": "album_2", "name": "Album Two"},
        ]
    }

    client.album_tracks.side_effect = [
        {
            "items": [
                {
                    "id": "track_1",
                    "name": "Song One",
                    "duration_ms": 180000,
                    "explicit": False,
                }
            ]
        },
        {
            "items": [
                {
                    "id": "track_2",
                    "name": "Song Two",
                    "duration_ms": 240000,
                    "explicit": True,
                }
            ]
        },
    ]

    return client


@patch("src.ingestion.extract_local.get_spotify_client")
def test_extract_returns_dataframe(mock_get_client, mock_spotify_client):
    mock_get_client.return_value = mock_spotify_client

    df = extract()

    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@patch("src.ingestion.extract_local.get_spotify_client")
def test_extract_contains_expected_columns(mock_get_client, mock_spotify_client):
    mock_get_client.return_value = mock_spotify_client

    df = extract()

    expected_columns = {
        "artist",
        "album_name",
        "track_name",
        "track_id",
        "duration_ms",
        "explicit",
    }

    assert expected_columns.issubset(df.columns)


@patch("src.ingestion.extract_local.get_spotify_client")
def test_extract_handles_multiple_albums(mock_get_client, mock_spotify_client):
    mock_get_client.return_value = mock_spotify_client

    df = extract()

    # Two albums, one track each
    assert len(df) == 2


@patch("src.ingestion.extract_local.get_spotify_client")
def test_extract_handles_empty_album_gracefully(mock_get_client):
    client = Mock()
    client.artist.return_value = {"id": "a1", "name": "Artist"}
    client.artist_albums.return_value = {
        "items": [{"id": "album_1", "name": "Empty Album"}]
    }
    client.album_tracks.return_value = {"items": []}

    mock_get_client.return_value = client

    df = extract()

    assert isinstance(df, pd.DataFrame)
