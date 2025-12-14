"""
Unit tests for Lambda transform: lambda/spotify_lambda_transform_ingest.py

This Lambda is triggered by S3 PUT events on spotify/processed/*.csv.
It enriches rows with:
- duration_min (from duration_ms)
- load_timestamp_utc (ISO timestamp)
and writes an output CSV to spotify/transformed/*_transformed.csv

Tests are intentionally small and focused (portfolio-friendly).
"""

import csv
import io
import urllib.parse
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

import sys
sys.path.insert(0, "./lambda")

from spotify_lambda_transform_ingest import transform_rows, lambda_handler  # noqa: E402


# -------------------------
# transform_rows tests
# -------------------------

def test_transform_rows_adds_duration_min_and_timestamp():
    rows = [{"track_id": "t1", "duration_ms": "200040"}]  # string from CSV
    out = list(transform_rows(rows))

    assert out[0]["duration_min"] == 3.33
    assert "load_timestamp_utc" in out[0]
    # should be ISO-ish
    datetime.fromisoformat(out[0]["load_timestamp_utc"])


def test_transform_rows_handles_invalid_duration():
    rows = [{"track_id": "t1", "duration_ms": "invalid"}]
    out = list(transform_rows(rows))

    assert out[0]["duration_min"] is None
    assert "load_timestamp_utc" in out[0]


def test_transform_rows_preserves_original_columns():
    rows = [{"artist": "A", "track_name": "S", "duration_ms": "180000"}]
    out = list(transform_rows(rows))

    assert out[0]["artist"] == "A"
    assert out[0]["track_name"] == "S"
    assert "duration_min" in out[0]


# -------------------------
# lambda_handler tests
# -------------------------

@pytest.fixture
def sample_csv_content():
    return (
        "artist,track_name,duration_ms\n"
        "Artist A,Song A,180000\n"
        "Artist B,Song B,240000\n"
    )


def _make_event(bucket: str, key: str):
    return {
        "Records": [
            {"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}
        ]
    }


@patch("spotify_lambda_transform_ingest.s3.put_object")
@patch("spotify_lambda_transform_ingest.s3.get_object")
def test_lambda_handler_processes_csv(mock_get, mock_put, sample_csv_content):
    mock_get.return_value = {"Body": Mock(read=lambda: sample_csv_content.encode("utf-8"))}
    mock_put.return_value = {}

    event = _make_event("test-bucket", "spotify/processed/test.csv")
    resp = lambda_handler(event, {})

    assert resp["status"] == "ok"
    mock_get.assert_called_once()
    mock_put.assert_called_once()

    out_key = mock_put.call_args.kwargs["Key"]
    assert out_key == "spotify/transformed/test_transformed.csv"

    out_body = mock_put.call_args.kwargs["Body"].decode("utf-8")
    rows = list(csv.DictReader(io.StringIO(out_body)))
    assert len(rows) == 2
    assert "duration_min" in rows[0]
    assert "load_timestamp_utc" in rows[0]


@patch("spotify_lambda_transform_ingest.s3.put_object")
@patch("spotify_lambda_transform_ingest.s3.get_object")
def test_lambda_handler_skips_non_csv(mock_get, mock_put):
    event = _make_event("test-bucket", "spotify/processed/data.json")
    resp = lambda_handler(event, {})

    mock_get.assert_not_called()
    mock_put.assert_not_called()
    # handler may return ok or None; we just ensure no crash
    assert resp is None or resp.get("status") in ("ok", "skipped")


@patch("spotify_lambda_transform_ingest.s3.put_object")
@patch("spotify_lambda_transform_ingest.s3.get_object")
def test_lambda_handler_skips_wrong_prefix(mock_get, mock_put):
    event = _make_event("test-bucket", "spotify/raw/test.csv")
    resp = lambda_handler(event, {})

    mock_get.assert_not_called()
    mock_put.assert_not_called()
    assert resp is None or resp.get("status") in ("ok", "skipped")


@patch("spotify_lambda_transform_ingest.s3.put_object")
@patch("spotify_lambda_transform_ingest.s3.get_object")
def test_lambda_handler_decodes_url_encoded_key(mock_get, mock_put, sample_csv_content):
    mock_get.return_value = {"Body": Mock(read=lambda: sample_csv_content.encode("utf-8"))}
    mock_put.return_value = {}

    encoded_key = urllib.parse.quote_plus("spotify/processed/tracks data.csv")
    event = _make_event("test-bucket", encoded_key)

    lambda_handler(event, {})

    called_key = mock_get.call_args.kwargs["Key"]
    assert called_key == "spotify/processed/tracks data.csv"


@patch("spotify_lambda_transform_ingest.s3.put_object")
@patch("spotify_lambda_transform_ingest.s3.get_object")
def test_lambda_handler_sets_content_type(mock_get, mock_put, sample_csv_content):
    mock_get.return_value = {"Body": Mock(read=lambda: sample_csv_content.encode("utf-8"))}
    mock_put.return_value = {}

    event = _make_event("test-bucket", "spotify/processed/test.csv")
    lambda_handler(event, {})

    assert mock_put.call_args.kwargs["ContentType"] == "text/csv"
