"""Configuration loading.

Config can come from a YAML file, CLI flags, or sensible defaults.
Precedence: CLI flags > YAML file > defaults.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

from .urls import default_months

MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
VALID_COLORS = {"yellow", "green"}


@dataclass
class Config:
    months: list[str]
    percentile: float = 0.9
    taxi_color: str = "yellow"
    output_dir: Path = Path("output")
    force: bool = False

    def __post_init__(self) -> None:
        # Validate months: must be YYYY-MM strings
        for m in self.months:
            if not MONTH_RE.match(m):
                raise ValueError(f"Invalid month {m!r}; expected YYYY-MM format")
        if not 0 < self.percentile < 1:
            raise ValueError(f"percentile must be in (0, 1); got {self.percentile}")
        if self.taxi_color not in VALID_COLORS:
            raise ValueError(f"taxi_color must be one of {VALID_COLORS}; got {self.taxi_color!r}")
        self.output_dir = Path(self.output_dir)


def load_config(
    yaml_path: Optional[Path] = None,
    months: Optional[list[str]] = None,
    percentile: Optional[float] = None,
    taxi_color: Optional[str] = None,
    output_dir: Optional[Path] = None,
    force: Optional[bool] = None,
) -> Config:
    """Load config with precedence: explicit kwargs > yaml > defaults."""
    data: dict = {}
    if yaml_path and yaml_path.exists():
        data = yaml.safe_load(yaml_path.read_text()) or {}

    # Apply overrides only when explicitly provided
    if months is not None:
        data["months"] = months
    if percentile is not None:
        data["percentile"] = percentile
    if taxi_color is not None:
        data["taxi_color"] = taxi_color
    if output_dir is not None:
        data["output_dir"] = output_dir
    if force is not None:
        data["force"] = force

    # Defaults for anything still missing
    if "months" not in data or not data["months"]:
        data["months"] = default_months(count=12)

    return Config(**data)
