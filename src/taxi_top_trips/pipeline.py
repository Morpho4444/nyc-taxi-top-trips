"""Pipeline: process one or more months of TLC data.

For each month:
  1. Compute the percentile threshold AND total trip count in a single pass.
  2. COPY filtered rows (trip_distance > threshold) to a partitioned parquet.
  3. Write a sidecar _stats.parquet with the threshold and counts.

At the end, glob all _stats.parquet files into a single output/summary.parquet
so the user has one place to look for results.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path

import duckdb

from .config import Config
from .urls import build_url

logger = logging.getLogger(__name__)


def _connect() -> duckdb.DuckDBPyConnection:
    """Connect to in-memory DuckDB with httpfs enabled for remote parquet reads.

    Try LOAD first (works if the extension is already cached locally); fall
    back to INSTALL+LOAD only if needed. This avoids a network round-trip
    to extensions.duckdb.org on every run and also lets the pipeline run
    against purely-local parquet without internet access.
    """
    con = duckdb.connect()
    con.execute("SET enable_progress_bar = false;")
    try:
        con.execute("LOAD httpfs;")
    except duckdb.Error:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except duckdb.Error as e:
            logger.warning(
                "could not load httpfs extension (%s); "
                "remote URLs will fail, but local parquet still works.", e,
            )
    return con


def process_month(
    con: duckdb.DuckDBPyConnection,
    year_month: str,
    percentile: float,
    taxi_color: str,
    output_dir: Path,
    force: bool = False,
) -> dict | None:
    """Process one month. Returns stats dict, or None if skipped/missing."""
    source_url = build_url(year_month, taxi_color)
    partition_dir = output_dir / "top_trips" / f"year_month={year_month}"
    out_parquet = partition_dir / "part.parquet"
    stats_parquet = partition_dir / "_stats.parquet"

    # Idempotency: skip if both outputs already exist and we're not forcing.
    if not force and out_parquet.exists() and stats_parquet.exists():
        logger.info("skip %s (already processed; use --force to redo)", year_month)
        return None

    partition_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    # Wrap every network-touching step in one try block. duckdb.IOException is
    # the parent of HTTPException and also covers other I/O failures (timeouts,
    # DNS, partial reads). A 403/404 from CloudFront on an unpublished month is
    # the common case — log a warning, leave the partition dir empty, and let
    # the caller continue with the next month.
    try:
        # 1. One-pass threshold + total. DuckDB streams the parquet from CloudFront.
        threshold, total_trips = con.execute(
            f"""
            SELECT
                quantile_cont(trip_distance, {percentile}) AS p_threshold,
                COUNT(*) AS total_trips
            FROM read_parquet('{source_url}')
            """
        ).fetchone()

        if threshold is None:
            logger.warning("skip %s (no rows in source)", year_month)
            return None

        # 2. Filter and write. We re-read the source; in practice DuckDB + CloudFront
        #    handle this efficiently. If this ever becomes a bottleneck, swap to a
        #    local cache in data/raw/ (intentionally not done here to keep the MVP small).
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet('{source_url}')
                WHERE trip_distance > {threshold}
            ) TO '{out_parquet}' (FORMAT PARQUET);
            """
        )

        filtered_trips = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out_parquet}')"
        ).fetchone()[0]

        elapsed = time.time() - t0

        # 3. Stats sidecar. One row per processed month, also acts as the "done" marker.
        con.execute(
            f"""
            COPY (
                SELECT
                    '{year_month}' AS year_month,
                    '{source_url}' AS source_url,
                    '{taxi_color}' AS taxi_color,
                    CAST({percentile} AS DOUBLE) AS percentile,
                    CAST({threshold} AS DOUBLE) AS threshold_miles,
                    CAST({total_trips} AS BIGINT) AS total_trips,
                    CAST({filtered_trips} AS BIGINT) AS filtered_trips,
                    CAST({elapsed} AS DOUBLE) AS runtime_seconds,
                    NOW() AS processed_at
            ) TO '{stats_parquet}' (FORMAT PARQUET);
            """
        )
    except duckdb.IOException as e:
        logger.warning("skip %s (source not reachable: %s)", year_month, e)
        # Don't leave a partial part.parquet around — it would confuse a re-run.
        if out_parquet.exists():
            out_parquet.unlink()
        return None

    logger.info(
        "done %s: threshold=%.2fmi total=%s filtered=%s (%.1f%%) in %.1fs",
        year_month, threshold, f"{total_trips:,}", f"{filtered_trips:,}",
        100 * filtered_trips / total_trips if total_trips else 0, elapsed,
    )
    return {
        "year_month": year_month,
        "threshold_miles": threshold,
        "total_trips": total_trips,
        "filtered_trips": filtered_trips,
        "runtime_seconds": elapsed,
    }


def build_summary(con: duckdb.DuckDBPyConnection, output_dir: Path) -> Path | None:
    """Glob all per-partition _stats.parquet into a single summary.parquet."""
    stats_glob = output_dir / "top_trips" / "year_month=*" / "_stats.parquet"
    summary_path = output_dir / "summary.parquet"
    # Check if any stats files exist
    matches = list((output_dir / "top_trips").glob("year_month=*/_stats.parquet"))
    if not matches:
        logger.warning("no stats files found; skipping summary")
        return None
    con.execute(
        f"""
        COPY (
            SELECT * FROM read_parquet('{stats_glob}')
            ORDER BY year_month
        ) TO '{summary_path}' (FORMAT PARQUET);
        """
    )
    logger.info("wrote summary: %s (%d months)", summary_path, len(matches))
    return summary_path


def run(config: Config) -> None:
    """Process every month in the config and write the summary."""
    config.output_dir.mkdir(parents=True, exist_ok=True)
    con = _connect()

    logger.info("processing %d months: %s ... %s",
                len(config.months), config.months[0], config.months[-1])

    processed = 0
    skipped = 0
    for ym in config.months:
        result = process_month(
            con,
            year_month=ym,
            percentile=config.percentile,
            taxi_color=config.taxi_color,
            output_dir=config.output_dir,
            force=config.force,
        )
        if result is None:
            skipped += 1
        else:
            processed += 1

    build_summary(con, config.output_dir)
    logger.info("finished: %d processed, %d skipped", processed, skipped)