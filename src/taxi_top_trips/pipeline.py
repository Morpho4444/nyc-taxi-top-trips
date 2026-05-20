"""Pipeline: process one or more months of TLC data.

For each month:
  1. Compute the percentile threshold, total count, and data-quality metrics
     in a single pass over the source parquet.
  2. COPY filtered rows (trip_distance > threshold) to a partitioned parquet.
  3. Write a sidecar _stats.parquet with the threshold and counts.
  4. Write a sidecar _data_quality.parquet with anomaly counts (no filtering).

At the end, glob all sidecars into top-level summary.parquet and
data_quality.parquet so the user has one place to look for each.
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
    dq_parquet = partition_dir / "_data_quality.parquet"

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
        # 1. Compute every aggregate in one pass into a temp table. Subsequent
        #    queries reference this table rather than re-reading the parquet,
        #    and Python never touches the aggregate values (avoiding fragile
        #    f-string interpolation of DB-typed values back into SQL).
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE _month_agg AS
            SELECT
                quantile_cont(trip_distance, {percentile})            AS p_threshold,
                COUNT(*)                                               AS total_trips,
                SUM(CASE WHEN trip_distance IS NULL THEN 1 ELSE 0 END) AS n_null,
                SUM(CASE WHEN trip_distance < 0    THEN 1 ELSE 0 END)  AS n_negative,
                SUM(CASE WHEN trip_distance = 0    THEN 1 ELSE 0 END)  AS n_zero,
                SUM(CASE WHEN trip_distance > 100  THEN 1 ELSE 0 END)  AS n_over_100mi,
                SUM(CASE WHEN trip_distance > 1000 THEN 1 ELSE 0 END)  AS n_over_1000mi,
                MIN(trip_distance)                                     AS min_distance,
                MAX(trip_distance)                                     AS max_distance
            FROM read_parquet('{source_url}');
            """
        )

        # Need threshold and total_trips on the Python side: threshold for the
        # logger output, total_trips for the percentage calc. Everything else
        # stays in SQL.
        threshold, total_trips = con.execute(
            "SELECT p_threshold, total_trips FROM _month_agg"
        ).fetchone()

        if threshold is None:
            logger.warning("skip %s (no rows in source)", year_month)
            return None

        # 2. Filter and write. The threshold lives in _month_agg, so we reference
        #    it via a scalar subquery rather than interpolating its value.
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet('{source_url}')
                WHERE trip_distance > (SELECT p_threshold FROM _month_agg)
            ) TO '{out_parquet}' (FORMAT PARQUET);
            """
        )

        filtered_trips = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out_parquet}')"
        ).fetchone()[0]

        elapsed = time.time() - t0

        # 3. Stats sidecar. Reads threshold and total from _month_agg; only
        #    the Python-known values (year_month, source_url, taxi_color,
        #    percentile, filtered_trips, elapsed) are interpolated.
        con.execute(
            f"""
            COPY (
                SELECT
                    '{year_month}'  AS year_month,
                    '{source_url}'  AS source_url,
                    '{taxi_color}'  AS taxi_color,
                    {percentile}    AS percentile,
                    a.p_threshold   AS threshold_miles,
                    a.total_trips   AS total_trips,
                    {filtered_trips} AS filtered_trips,
                    {elapsed}       AS runtime_seconds,
                    NOW()           AS processed_at
                FROM _month_agg a
            ) TO '{stats_parquet}' (FORMAT PARQUET);
            """
        )

        # 4. Data quality sidecar — reads everything from _month_agg, no
        #    Python interpolation of aggregate values. Surfaces anomalies
        #    (NULL/negative/zero/extreme distances) without filtering.
        con.execute(
            f"""
            COPY (
                SELECT
                    '{year_month}' AS year_month,
                    total_trips,
                    n_null         AS n_null_distance,
                    n_negative     AS n_negative_distance,
                    n_zero         AS n_zero_distance,
                    n_over_100mi,
                    n_over_1000mi,
                    min_distance,
                    max_distance
                FROM _month_agg
            ) TO '{dq_parquet}' (FORMAT PARQUET);
            """
        )

        # Fetch the data quality counts for the return dict / logging.
        dq = con.execute(
            """SELECT n_null, n_negative, n_zero, n_over_100mi, n_over_1000mi,
                      min_distance, max_distance
               FROM _month_agg"""
        ).fetchone()
        n_null, n_negative, n_zero, n_over_100mi, n_over_1000mi, min_distance, max_distance = dq

    except duckdb.IOException as e:
        logger.warning("skip %s (source not reachable: %s)", year_month, e)
        if out_parquet.exists():
            out_parquet.unlink()
        return None
    except duckdb.Error as e:
        # Catch any other DuckDB error (Parser, Binder, Conversion, etc.) so a
        # single malformed month doesn't kill the whole run. Log enough detail
        # to diagnose later, but keep going.
        logger.error("skip %s (DuckDB error: %s: %s)", year_month, type(e).__name__, e)
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
        "n_null_distance": n_null,
        "n_negative_distance": n_negative,
        "n_zero_distance": n_zero,
        "n_over_100mi": n_over_100mi,
        "n_over_1000mi": n_over_1000mi,
        "min_distance": min_distance,
        "max_distance": max_distance,
    }


def build_summary(con: duckdb.DuckDBPyConnection, output_dir: Path) -> Path | None:
    """Glob per-partition sidecars into top-level summary.parquet + data_quality.parquet."""
    top_trips = output_dir / "top_trips"

    # Stats summary
    stats_matches = list(top_trips.glob("year_month=*/_stats.parquet"))
    summary_path: Path | None = None
    if stats_matches:
        stats_glob = top_trips / "year_month=*" / "_stats.parquet"
        summary_path = output_dir / "summary.parquet"
        con.execute(
            f"""
            COPY (
                SELECT * FROM read_parquet('{stats_glob}')
                ORDER BY year_month
            ) TO '{summary_path}' (FORMAT PARQUET);
            """
        )
        logger.info("wrote summary: %s (%d months)", summary_path, len(stats_matches))
    else:
        logger.warning("no stats files found; skipping summary")

    # Data quality summary (independent — older partitions may not have one)
    dq_matches = list(top_trips.glob("year_month=*/_data_quality.parquet"))
    if dq_matches:
        dq_glob = top_trips / "year_month=*" / "_data_quality.parquet"
        dq_path = output_dir / "data_quality.parquet"
        con.execute(
            f"""
            COPY (
                SELECT * FROM read_parquet('{dq_glob}')
                ORDER BY year_month
            ) TO '{dq_path}' (FORMAT PARQUET);
            """
        )
        logger.info("wrote data_quality: %s (%d months)", dq_path, len(dq_matches))

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
