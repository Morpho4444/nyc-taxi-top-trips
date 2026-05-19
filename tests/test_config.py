"""Tests for config loading and validation."""
import pytest

from taxi_top_trips.config import Config, load_config


def test_config_defaults():
    c = Config(months=["2024-01"])
    assert c.percentile == 0.9
    assert c.taxi_color == "yellow"


def test_config_rejects_bad_month():
    with pytest.raises(ValueError, match="Invalid month"):
        Config(months=["2024-1"])  # missing leading zero


def test_config_rejects_bad_percentile():
    with pytest.raises(ValueError, match="percentile"):
        Config(months=["2024-01"], percentile=1.5)


def test_config_rejects_bad_color():
    with pytest.raises(ValueError, match="taxi_color"):
        Config(months=["2024-01"], taxi_color="purple")


def test_load_config_cli_overrides_yaml(tmp_path):
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("months: ['2024-01']\npercentile: 0.9\n")
    c = load_config(yaml_path=yaml_path, percentile=0.95)
    assert c.percentile == 0.95
    assert c.months == ["2024-01"]


def test_load_config_defaults_months_when_missing(tmp_path):
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("percentile: 0.9\n")
    c = load_config(yaml_path=yaml_path)
    assert len(c.months) == 12  # default
