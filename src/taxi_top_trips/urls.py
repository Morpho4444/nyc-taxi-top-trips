"""URL building and default month-list logic.

TLC publishes monthly parquet files with a ~2 month delay. Default behavior
is to compute the last N months ending at (today - 2 months) so that
re-running the pipeline a few months later just picks up new months.
"""
from __future__ import annotations

from datetime import date

CLOUDFRONT_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def default_months(today: date | None = None, count: int = 12, delay_months: int = 2) -> list[str]:
    """Return the N most recent likely-published months as YYYY-MM strings, oldest first.

    Args:
        today: pin the "current" date; defaults to date.today().
        count: how many months to return.
        delay_months: TLC publishing lag; the latest month returned is (today - delay_months).
    """
    today = today or date.today()
    y, m = today.year, today.month - delay_months
    while m < 1:
        m += 12
        y -= 1

    months: list[str] = []
    for _ in range(count):
        months.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m < 1:
            m += 12
            y -= 1
    return list(reversed(months))


def build_url(year_month: str, taxi_color: str = "yellow") -> str:
    """Build the canonical TLC CloudFront URL for a given month and taxi color."""
    return f"{CLOUDFRONT_BASE}/{taxi_color}_tripdata_{year_month}.parquet"
