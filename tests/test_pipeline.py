"""End-to-end pipeline test using a local synthetic parquet.

Monkeypatches build_url so process_month points at a tmp file instead of
hitting CloudFront, but the actual processing code path is exercised in full.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from taxi_top_trips import pipeline
from taxi_top_trips.config import Config


def test_process_month_end_to_end(synthetic_parquet: Path, tmp_path: Path, monkeypatch):
    monkeypatch.setattr(pipeline, "build_url", lambda ym, color: str(synthetic_parquet))

    con = duckdb.connect()  # no httpfs needed for local file
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    result = pipeline.process_month(
        con,
        year_month="2024-01",
        percentile=0.9,
        taxi_color="yellow",
        output_dir=output_dir,
        force=False,
    )

    assert result is not None
    assert result["total_trips"] == 10_000
    # >p90 should be roughly 10% of rows; allow tolerance for tie handling
    assert 800 <= result["filtered_trips"] <= 1200
    assert result["threshold_miles"] > 0

    # Outputs exist
    part = output_dir / "top_trips" / "year_month=2024-01" / "part.parquet"
    stats = output_dir / "top_trips" / "year_month=2024-01" / "_stats.parquet"
    assert part.exists()
    assert stats.exists()

    # Filtered rows are actually above threshold
    min_dist = con.execute(
        f"SELECT MIN(trip_distance) FROM read_parquet('{part}')"
    ).fetchone()[0]
    assert min_dist > result["threshold_miles"]


def test_idempotency_skips_existing(synthetic_parquet: Path, tmp_path: Path, monkeypatch):
    monkeypatch.setattr(pipeline, "build_url", lambda ym, color: str(synthetic_parquet))
    con = duckdb.connect()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # First run: does the work
    r1 = pipeline.process_month(con, "2024-01", 0.9, "yellow", output_dir, force=False)
    assert r1 is not None

    # Second run: skipped
    r2 = pipeline.process_month(con, "2024-01", 0.9, "yellow", output_dir, force=False)
    assert r2 is None

    # Forced: redone
    r3 = pipeline.process_month(con, "2024-01", 0.9, "yellow", output_dir, force=True)
    assert r3 is not None


def test_build_summary(synthetic_parquet: Path, tmp_path: Path, monkeypatch):
    monkeypatch.setattr(pipeline, "build_url", lambda ym, color: str(synthetic_parquet))
    con = duckdb.connect()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    pipeline.process_month(con, "2024-01", 0.9, "yellow", output_dir)
    pipeline.process_month(con, "2024-02", 0.9, "yellow", output_dir)

    summary_path = pipeline.build_summary(con, output_dir)
    assert summary_path is not None and summary_path.exists()

    rows = con.execute(f"SELECT year_month FROM read_parquet('{summary_path}') ORDER BY year_month").fetchall()
    assert [r[0] for r in rows] == ["2024-01", "2024-02"]


def test_missing_source_is_skipped(tmp_path: Path, monkeypatch):
    """Simulates the real-world case of a month whose parquet isn't published yet
    (HTTP 403/404 from CloudFront). The loop should skip and continue, not crash."""
    monkeypatch.setattr(pipeline, "build_url", lambda ym, color: str(tmp_path / "does_not_exist.parquet"))

    con = duckdb.connect()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    result = pipeline.process_month(con, "2099-12", 0.9, "yellow", output_dir)
    assert result is None  # skipped, not raised

    # No stray part.parquet left behind
    part = output_dir / "top_trips" / "year_month=2099-12" / "part.parquet"
    assert not part.exists()


def test_run_continues_when_some_months_missing(synthetic_parquet: Path, tmp_path: Path, monkeypatch):
    """Mix of available and unavailable months. Pipeline processes what it can."""
    missing = tmp_path / "missing.parquet"

    def fake_url(ym, color):
        return str(synthetic_parquet) if ym in {"2024-01", "2024-02"} else str(missing)

    monkeypatch.setattr(pipeline, "build_url", fake_url)
    config = Config(
        months=["2024-01", "2024-02", "2099-11", "2099-12"],
        percentile=0.9,
        taxi_color="yellow",
        output_dir=tmp_path / "output",
    )
    pipeline.run(config)

    summary = tmp_path / "output" / "summary.parquet"
    assert summary.exists()
    con = duckdb.connect()
    rows = con.execute(f"SELECT year_month FROM read_parquet('{summary}') ORDER BY year_month").fetchall()
    assert [r[0] for r in rows] == ["2024-01", "2024-02"]


def test_run_full_pipeline(synthetic_parquet: Path, tmp_path: Path, monkeypatch):
    monkeypatch.setattr(pipeline, "build_url", lambda ym, color: str(synthetic_parquet))
    config = Config(
        months=["2024-01", "2024-02", "2024-03"],
        percentile=0.9,
        taxi_color="yellow",
        output_dir=tmp_path / "output",
    )
    pipeline.run(config)

    summary = tmp_path / "output" / "summary.parquet"
    assert summary.exists()
    con = duckdb.connect()
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{summary}')").fetchone()[0]
    assert n == 3


def test_data_quality_counts_correctly(tmp_path: Path, monkeypatch):
    """Build a parquet with known anomalies and verify the counts match exactly."""
    import numpy as np
    import pandas as pd

    # Deliberate composition: 100 clean, 5 zero, 3 negative, 2 NULL,
    # 2 over 100mi, 1 over 1000mi
    distances = (
        list(np.linspace(0.5, 20.0, 100))   # 100 clean
        + [0.0] * 5                          # 5 zero
        + [-1.0, -2.0, -0.5]                 # 3 negative
        + [None, None]                       # 2 NULL
        + [150.0, 200.0]                     # 2 over 100mi (and counted once each)
        + [9999.0]                           # 1 over 1000mi (also counted in over_100mi)
    )
    df = pd.DataFrame({"trip_distance": distances})
    fixture = tmp_path / "dirty.parquet"
    df.to_parquet(fixture)

    monkeypatch.setattr(pipeline, "build_url", lambda ym, color: str(fixture))
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    con = duckdb.connect()

    result = pipeline.process_month(con, "2024-01", 0.9, "yellow", output_dir)
    assert result is not None
    assert result["total_trips"] == 113   # 100 + 5 + 3 + 2 + 2 + 1
    assert result["n_zero_distance"] == 5
    assert result["n_negative_distance"] == 3
    assert result["n_null_distance"] == 2
    assert result["n_over_100mi"] == 3    # 150, 200, 9999 all > 100
    assert result["n_over_1000mi"] == 1   # only 9999
    assert result["max_distance"] == 9999.0

    # Verify the sidecar exists and is readable
    dq_parquet = output_dir / "top_trips" / "year_month=2024-01" / "_data_quality.parquet"
    assert dq_parquet.exists()
    dq_row = con.execute(f"SELECT * FROM read_parquet('{dq_parquet}')").fetchone()
    assert dq_row is not None


def test_build_summary_writes_data_quality_aggregate(synthetic_parquet: Path, tmp_path: Path, monkeypatch):
    monkeypatch.setattr(pipeline, "build_url", lambda ym, color: str(synthetic_parquet))
    con = duckdb.connect()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    pipeline.process_month(con, "2024-01", 0.9, "yellow", output_dir)
    pipeline.process_month(con, "2024-02", 0.9, "yellow", output_dir)
    pipeline.build_summary(con, output_dir)

    dq_path = output_dir / "data_quality.parquet"
    assert dq_path.exists()
    rows = con.execute(
        f"SELECT year_month FROM read_parquet('{dq_path}') ORDER BY year_month"
    ).fetchall()
    assert [r[0] for r in rows] == ["2024-01", "2024-02"]