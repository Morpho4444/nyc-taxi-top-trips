"""Shared test fixtures."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def synthetic_parquet(tmp_path: Path) -> Path:
    """Tiny parquet with a known distance distribution for deterministic tests."""
    rng = np.random.default_rng(42)
    n = 10_000
    df = pd.DataFrame({
        "VendorID": rng.integers(1, 3, n),
        "tpep_pickup_datetime": pd.to_datetime("2024-01-01")
            + pd.to_timedelta(rng.integers(0, 30 * 24 * 3600, n), unit="s"),
        "tpep_dropoff_datetime": pd.to_datetime("2024-01-01")
            + pd.to_timedelta(rng.integers(0, 30 * 24 * 3600, n), unit="s"),
        "passenger_count": rng.integers(1, 6, n),
        "trip_distance": rng.lognormal(mean=0.8, sigma=0.9, size=n).round(2),
        "fare_amount": rng.uniform(3, 80, n).round(2),
    })
    out = tmp_path / "yellow_tripdata_2024-01.parquet"
    df.to_parquet(out)
    return out
