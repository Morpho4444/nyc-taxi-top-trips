"""CLI entrypoint.

    python -m taxi_top_trips                      # use config.yaml + defaults
    python -m taxi_top_trips --months 2024-01 2024-02
    python -m taxi_top_trips --percentile 0.95
    python -m taxi_top_trips --force              # reprocess even if outputs exist
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config import load_config
from .pipeline import run


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="taxi-top-trips",
        description="Extract trips above the Nth percentile of trip_distance from NYC TLC parquet files.",
    )
    p.add_argument("--config", type=Path, default=Path("config.yaml"),
                   help="Path to YAML config (default: config.yaml).")
    p.add_argument("--months", nargs="+", default=None, metavar="YYYY-MM",
                   help="Override months. Example: --months 2024-01 2024-02")
    p.add_argument("--percentile", type=float, default=None,
                   help="Percentile threshold in (0, 1). Default 0.9.")
    p.add_argument("--taxi-color", choices=["yellow", "green"], default=None,
                   help="Taxi color. Default yellow.")
    p.add_argument("--output-dir", type=Path, default=None,
                   help="Output directory. Default output/")
    p.add_argument("--force", action="store_true", default=None,
                   help="Reprocess months even if their outputs already exist.")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Enable debug logging.")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        config = load_config(
            yaml_path=args.config,
            months=args.months,
            percentile=args.percentile,
            taxi_color=args.taxi_color,
            output_dir=args.output_dir,
            force=args.force,
        )
        run(config)
    except Exception as e:
        logging.error("pipeline failed: %s", e)
        if args.verbose:
            raise
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
