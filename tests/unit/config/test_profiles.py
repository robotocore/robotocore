"""Unit tests for configuration profiles."""

import os
from pathlib import Path

import pytest

from robotocore.config.profiles import (
    get_active_profiles,
    get_config_dir,
    list_available_profiles,
    load_profiles,
    parse_dotenv,
)


@pytest.fixture()
def config_dir(tmp_path: Path) -> Path:
    """Create a temporary config directory."""
    return tmp_path / ".robotocore"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove profile-related env vars before each test."""
    monkeypatch.delenv("CONFIG_PROFILE", raising=False)
    monkeypatch.delenv("ROBOTOCORE_CONFIG_DIR", raising=False)


# ---------------------------------------------------------------------------
# Dotenv parsing
# ---------------------------------------------------------------------------


def test_parse_key_value() -> None:
    content = "FOO=bar\nBAZ=qux"
    result = parse_dotenv(content)
    assert result == {"FOO": "bar", "BAZ": "qux"}


def test_parse_comments_ignored() -> None:
    content = "# this is a comment\nFOO=bar\n# another comment"
    result = parse_dotenv(content)
    assert result == {"FOO": "bar"}


def test_parse_empty_lines_ignored() -> None:
    content = "FOO=bar\n\n\nBAZ=qux\n"
    result = parse_dotenv(content)
    assert result == {"FOO": "bar", "BAZ": "qux"}


def test_parse_quoted_values() -> None:
    content = "FOO=\"value with spaces\"\nBAR='single quoted'"
    result = parse_dotenv(content)
    assert result == {"FOO": "value with spaces", "BAR": "single quoted"}


def test_parse_variable_expansion() -> None:
    content = "BASE=/opt\nPATH_VAR=${BASE}/bin"
    result = parse_dotenv(content)
    assert result == {"BASE": "/opt", "PATH_VAR": "/opt/bin"}


def test_parse_variable_expansion_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXISTING_VAR", "hello")
    content = "GREETING=${EXISTING_VAR}/world"
    result = parse_dotenv(content)
    assert result == {"GREETING": "hello/world"}


# ---------------------------------------------------------------------------
# Profile loading
# ---------------------------------------------------------------------------


def test_load_single_profile(config_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir.mkdir(parents=True)
    (config_dir / "testing.env").write_text("MY_VAR=testing_value\n")
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("CONFIG_PROFILE", "testing")

    load_profiles()

    assert os.environ["MY_VAR"] == "testing_value"


def test_load_multiple_profiles(config_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir.mkdir(parents=True)
    (config_dir / "base.env").write_text("A=from_base\nB=from_base\n")
    (config_dir / "override.env").write_text("B=from_override\nC=from_override\n")
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("CONFIG_PROFILE", "base,override")

    load_profiles()

    assert os.environ["A"] == "from_base"
    assert os.environ["B"] == "from_override"
    assert os.environ["C"] == "from_override"


def test_later_profile_overrides_earlier(config_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir.mkdir(parents=True)
    (config_dir / "first.env").write_text("KEY=first\n")
    (config_dir / "second.env").write_text("KEY=second\n")
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("CONFIG_PROFILE", "first,second")

    load_profiles()

    assert os.environ["KEY"] == "second"


def test_env_var_overrides_profile(config_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir.mkdir(parents=True)
    (config_dir / "testing.env").write_text("MY_KEY=from_profile\n")
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("CONFIG_PROFILE", "testing")
    monkeypatch.setenv("MY_KEY", "from_env")

    load_profiles()

    assert os.environ["MY_KEY"] == "from_env"


def test_default_env_loaded_automatically(
    config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir.mkdir(parents=True)
    (config_dir / "default.env").write_text("DEFAULT_VAR=default_value\n")
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))
    # No CONFIG_PROFILE set

    load_profiles()

    assert os.environ["DEFAULT_VAR"] == "default_value"


def test_named_profile_overrides_default(config_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir.mkdir(parents=True)
    (config_dir / "default.env").write_text("SHARED=default\n")
    (config_dir / "custom.env").write_text("SHARED=custom\n")
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("CONFIG_PROFILE", "custom")

    load_profiles()

    assert os.environ["SHARED"] == "custom"


def test_missing_profile_warns_no_crash(
    config_dir: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    config_dir.mkdir(parents=True)
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("CONFIG_PROFILE", "nonexistent")

    import logging

    with caplog.at_level(logging.WARNING):
        load_profiles()

    assert "nonexistent" in caplog.text


def test_missing_config_dir_created(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    new_dir = tmp_path / "new_config_dir"
    assert not new_dir.exists()
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(new_dir))

    config_d = get_config_dir()

    assert config_d == new_dir
    assert new_dir.exists()


def test_config_dir_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    custom_dir = tmp_path / "custom"
    custom_dir.mkdir()
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(custom_dir))

    assert get_config_dir() == custom_dir


def test_no_config_profile_only_default(config_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir.mkdir(parents=True)
    (config_dir / "default.env").write_text("ONLY_DEFAULT=yes\n")
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))

    load_profiles()

    assert os.environ["ONLY_DEFAULT"] == "yes"


def test_list_available_profiles(config_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir.mkdir(parents=True)
    (config_dir / "default.env").write_text("A=1\n")
    (config_dir / "testing.env").write_text("B=2\n")
    (config_dir / "production.env").write_text("C=3\n")
    (config_dir / "not_a_profile.txt").write_text("D=4\n")
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))

    profiles = list_available_profiles()
    assert sorted(profiles) == ["default", "production", "testing"]


def test_get_active_profiles_empty(config_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir.mkdir(parents=True)
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))

    load_profiles()
    active = get_active_profiles()
    assert active == []


def test_get_active_profiles_with_default_and_named(
    config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir.mkdir(parents=True)
    (config_dir / "default.env").write_text("A=1\n")
    (config_dir / "dev.env").write_text("B=2\n")
    monkeypatch.setenv("ROBOTOCORE_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("CONFIG_PROFILE", "dev")

    load_profiles()
    active = get_active_profiles()
    assert active == ["default", "dev"]
