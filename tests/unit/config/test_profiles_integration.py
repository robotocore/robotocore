"""Semantic / integration tests for configuration profiles."""

import os
from pathlib import Path

import pytest

from robotocore.config.profiles import (
    get_active_profiles,
    get_resolved_config,
    load_profiles,
)


@pytest.fixture()
def config_dir(tmp_path: Path) -> Path:
    d = tmp_path / ".robotocore"
    d.mkdir()
    return d


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CONFIG_PROFILE", raising=False)
    monkeypatch.delenv("ROBOTOCORE_CONFIG_DIR", raising=False)


def test_e2e_write_load_verify(config_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Write a profile file, set CONFIG_PROFILE, load, verify env vars."""
    (config_dir / "myprofile.env").write_text("SERVICE_PORT=9999\nSERVICE_HOST=localhost\n")
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("CONFIG_PROFILE", "myprofile")

    load_profiles()

    assert os.environ["SERVICE_PORT"] == "9999"
    assert os.environ["SERVICE_HOST"] == "localhost"


def test_e2e_two_profiles_overlap_later_wins(
    config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two profiles with overlapping keys -- later profile wins."""
    (config_dir / "base.env").write_text("PORT=8080\nHOST=0.0.0.0\n")
    (config_dir / "dev.env").write_text("PORT=4566\nDEBUG=1\n")
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("CONFIG_PROFILE", "base,dev")

    load_profiles()

    assert os.environ["PORT"] == "4566"
    assert os.environ["HOST"] == "0.0.0.0"
    assert os.environ["DEBUG"] == "1"


def test_e2e_env_var_wins_over_profile(config_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Environment variable set before load overrides profile value."""
    (config_dir / "prod.env").write_text("LOG_LEVEL=WARNING\n")
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("CONFIG_PROFILE", "prod")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    load_profiles()

    assert os.environ["LOG_LEVEL"] == "DEBUG"


def test_management_endpoint_list_profiles(
    config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Management endpoint returns list of available profiles."""
    from robotocore.config.profiles import list_available_profiles

    (config_dir / "default.env").write_text("A=1\n")
    (config_dir / "staging.env").write_text("B=2\n")
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))

    profiles = list_available_profiles()
    assert "default" in profiles
    assert "staging" in profiles


def test_management_endpoint_active_config(
    config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Management endpoint returns active profiles and resolved config."""
    (config_dir / "default.env").write_text("BASE_URL=http://localhost\n")
    (config_dir / "test.env").write_text("BASE_URL=http://test\nTEST_MODE=1\n")
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("CONFIG_PROFILE", "test")

    load_profiles()

    active = get_active_profiles()
    assert active == ["default", "test"]

    resolved = get_resolved_config()
    assert resolved["BASE_URL"] == "http://test"
    assert resolved["TEST_MODE"] == "1"
