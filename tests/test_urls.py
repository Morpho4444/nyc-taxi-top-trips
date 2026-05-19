"""Tests for URL building and month list defaults."""
from datetime import date

from taxi_top_trips.urls import build_url, default_months


def test_default_months_returns_oldest_first():
    months = default_months(today=date(2025, 6, 15), count=12, delay_months=2)
    assert months[-1] == "2025-04"  # June - 2 months = April
    assert months[0] == "2024-05"   # 12 months back
    assert len(months) == 12


def test_default_months_handles_year_boundary():
    # February 2025 - 2 months = December 2024
    months = default_months(today=date(2025, 2, 10), count=3, delay_months=2)
    assert months == ["2024-10", "2024-11", "2024-12"]


def test_default_months_single():
    months = default_months(today=date(2025, 6, 15), count=1, delay_months=2)
    assert months == ["2025-04"]


def test_build_url_yellow():
    url = build_url("2024-03", "yellow")
    assert url == "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet"


def test_build_url_green():
    url = build_url("2024-03", "green")
    assert url.endswith("green_tripdata_2024-03.parquet")
