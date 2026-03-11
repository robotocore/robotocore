"""Unit tests for RuntimeConfig."""

import logging
import os
import time
from unittest.mock import patch

import pytest

from robotocore.config.runtime import (
    MUTABLE_SETTINGS,
    RuntimeConfig,
    get_runtime_config,
)


@pytest.fixture
def config():
    """Create a fresh RuntimeConfig for each test."""
    return RuntimeConfig()


@pytest.fixture
def enabled_config():
    """Create a RuntimeConfig with updates enabled."""
    with patch.dict(os.environ, {"ENABLE_CONFIG_UPDATES": "1"}):
        cfg = RuntimeConfig()
        yield cfg


class TestRuntimeConfigStorage:
    def test_stores_overrides(self, config):
        config._overrides["LOG_LEVEL"] = "DEBUG"
        assert config._overrides["LOG_LEVEL"] == "DEBUG"

    def test_get_returns_override_when_set(self, enabled_config):
        enabled_config.set("LOG_LEVEL", "DEBUG")
        assert enabled_config.get("LOG_LEVEL") == "DEBUG"

    def test_get_returns_env_var_when_no_override(self, config):
        with patch.dict(os.environ, {"LOG_LEVEL": "WARNING"}):
            assert config.get("LOG_LEVEL") == "WARNING"

    def test_get_returns_default_when_no_override_or_env(self, config):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("LOG_LEVEL", None)
            result = config.get("LOG_LEVEL", "INFO")
            assert result == "INFO"

    def test_delete_removes_override(self, enabled_config):
        enabled_config.set("LOG_LEVEL", "DEBUG")
        assert enabled_config.get("LOG_LEVEL") == "DEBUG"
        old = enabled_config.delete("LOG_LEVEL")
        assert old == "DEBUG"
        # After delete, should fall back to env or default
        with patch.dict(os.environ, {"LOG_LEVEL": "WARNING"}):
            assert enabled_config.get("LOG_LEVEL") == "WARNING"

    def test_delete_nonexistent_returns_none(self, config):
        result = config.delete("LOG_LEVEL")
        assert result is None


class TestWhitelist:
    def test_whitelisted_setting_accepted(self, enabled_config):
        enabled_config.set("LOG_LEVEL", "DEBUG")
        assert enabled_config.get("LOG_LEVEL") == "DEBUG"

    def test_non_whitelisted_setting_rejected(self, enabled_config):
        with pytest.raises(ValueError, match="cannot be changed at runtime"):
            enabled_config.set("PORT", "5000")

    def test_all_mutable_settings_in_whitelist(self):
        expected = {
            "LOG_LEVEL",
            "DEBUG",
            "ENFORCE_IAM",
            "AUDIT_LOG_SIZE",
            "SQS_DELAY_PURGE_RETRY",
            "SQS_DELAY_RECENTLY_DELETED",
            "DYNAMODB_REMOVE_EXPIRED_ITEMS",
            "USAGE_ANALYTICS",
        }
        assert set(MUTABLE_SETTINGS.keys()) == expected


class TestChangeHistory:
    def test_change_history_recorded(self, enabled_config):
        enabled_config.set("LOG_LEVEL", "DEBUG")
        history = enabled_config.get_history()
        assert len(history) == 1
        assert history[0]["key"] == "LOG_LEVEL"

    def test_change_history_includes_old_and_new_values(self, enabled_config):
        with patch.dict(os.environ, {"LOG_LEVEL": "INFO"}):
            enabled_config.set("LOG_LEVEL", "DEBUG")
            history = enabled_config.get_history()
            assert history[0]["old_value"] == "INFO"
            assert history[0]["new_value"] == "DEBUG"

    def test_change_history_has_timestamp(self, enabled_config):
        before = time.time()
        enabled_config.set("LOG_LEVEL", "DEBUG")
        after = time.time()
        history = enabled_config.get_history()
        assert before <= history[0]["timestamp"] <= after

    def test_multiple_changes_tracked(self, enabled_config):
        enabled_config.set("LOG_LEVEL", "DEBUG")
        enabled_config.set("ENFORCE_IAM", "1")
        history = enabled_config.get_history()
        assert len(history) == 2


class TestChangePropagation:
    def test_log_level_change_propagates_to_logger(self, enabled_config):
        enabled_config.set("LOG_LEVEL", "DEBUG")
        root_logger = logging.getLogger()
        assert root_logger.level == logging.DEBUG

    def test_reset_returns_to_original_value(self, enabled_config):
        with patch.dict(os.environ, {"LOG_LEVEL": "INFO"}):
            enabled_config.set("LOG_LEVEL", "DEBUG")
            assert enabled_config.get("LOG_LEVEL") == "DEBUG"
            enabled_config.delete("LOG_LEVEL")
            assert enabled_config.get("LOG_LEVEL") == "INFO"


class TestListConfig:
    def test_list_all_current_config_with_sources(self, enabled_config):
        with patch.dict(os.environ, {"LOG_LEVEL": "INFO", "DEBUG": "0"}):
            enabled_config.set("LOG_LEVEL", "DEBUG")
            all_config = enabled_config.list_all()
            # LOG_LEVEL should show as runtime override
            log_entry = next(e for e in all_config if e["key"] == "LOG_LEVEL")
            assert log_entry["value"] == "DEBUG"
            assert log_entry["source"] == "runtime"
            assert log_entry["mutable"] is True

            # DEBUG should show as env
            debug_entry = next(e for e in all_config if e["key"] == "DEBUG")
            assert debug_entry["value"] == "0"
            assert debug_entry["source"] == "env"


class TestEnableGate:
    def test_config_updates_disabled_by_default(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENABLE_CONFIG_UPDATES", None)
            cfg = RuntimeConfig()
            assert cfg.updates_enabled is False

    def test_config_updates_enabled_when_env_set(self):
        with patch.dict(os.environ, {"ENABLE_CONFIG_UPDATES": "1"}):
            cfg = RuntimeConfig()
            assert cfg.updates_enabled is True

    def test_set_raises_when_updates_disabled(self, config):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENABLE_CONFIG_UPDATES", None)
            with pytest.raises(PermissionError, match="disabled"):
                config.set("LOG_LEVEL", "DEBUG")


class TestSingleton:
    def test_get_runtime_config_returns_same_instance(self):
        a = get_runtime_config()
        b = get_runtime_config()
        assert a is b
