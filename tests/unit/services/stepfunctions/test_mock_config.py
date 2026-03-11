"""Unit tests for StepFunctions mock configuration loading and parsing."""

import json
import os
import time

import pytest

from robotocore.services.stepfunctions.mock_config import (
    extract_test_case_from_name,
    get_mock_config,
    get_state_machine_names,
    get_test_case,
    get_test_case_names,
    load_mock_config,
    reset_mock_config,
    resolve_mock_state,
)

VALID_CONFIG = {
    "StateMachines": {
        "MyStateMachine": {
            "TestCases": {
                "HappyPath": {
                    "StateA": {"Return": {"result": "success"}},
                    "StateB": {"Return": {"count": 42}},
                },
                "ErrorPath": {
                    "StateA": {"Return": {"result": "success"}},
                    "StateB": {
                        "Throw": {
                            "Error": "CustomError",
                            "Cause": "Something failed",
                        }
                    },
                },
            }
        },
        "OtherMachine": {
            "TestCases": {
                "Simple": {
                    "OnlyState": {"Return": {"ok": True}},
                }
            }
        },
    }
}


@pytest.fixture
def config_file(tmp_path):
    """Create a temp config file and return its path."""
    path = tmp_path / "sfn_mock.json"
    path.write_text(json.dumps(VALID_CONFIG))
    return str(path)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Ensure SFN_MOCK_CONFIG is not set and cached config is reset."""
    monkeypatch.delenv("SFN_MOCK_CONFIG", raising=False)
    reset_mock_config()
    yield
    reset_mock_config()


class TestLoadMockConfig:
    def test_load_valid_config(self, config_file):
        config = load_mock_config(config_file)
        assert config is not None
        assert "StateMachines" in config
        assert "MyStateMachine" in config["StateMachines"]

    def test_load_invalid_json(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("{not valid json}")
        with pytest.raises(ValueError, match="Invalid JSON"):
            load_mock_config(str(path))

    def test_load_nonexistent_file(self):
        result = load_mock_config("/nonexistent/path/to/config.json")
        assert result is None

    def test_load_from_env_var(self, config_file, monkeypatch):
        monkeypatch.setenv("SFN_MOCK_CONFIG", config_file)
        config = load_mock_config()
        assert config is not None
        assert "StateMachines" in config

    def test_load_no_path_no_env(self):
        config = load_mock_config()
        assert config is None


class TestConfigParsing:
    def test_get_state_machine_names(self):
        names = get_state_machine_names(VALID_CONFIG)
        assert sorted(names) == ["MyStateMachine", "OtherMachine"]

    def test_get_test_case_names(self):
        names = get_test_case_names(VALID_CONFIG, "MyStateMachine")
        assert sorted(names) == ["ErrorPath", "HappyPath"]

    def test_get_test_case_names_missing_machine(self):
        names = get_test_case_names(VALID_CONFIG, "NonexistentMachine")
        assert names == []

    def test_get_state_return_values(self):
        test_case = get_test_case(VALID_CONFIG, "MyStateMachine", "HappyPath")
        assert test_case is not None
        assert test_case["StateA"] == {"Return": {"result": "success"}}
        assert test_case["StateB"] == {"Return": {"count": 42}}

    def test_get_state_throw_values(self):
        test_case = get_test_case(VALID_CONFIG, "MyStateMachine", "ErrorPath")
        assert test_case is not None
        assert test_case["StateB"]["Throw"]["Error"] == "CustomError"
        assert test_case["StateB"]["Throw"]["Cause"] == "Something failed"


class TestTestCaseLookup:
    def test_lookup_by_name(self):
        test_case = get_test_case(VALID_CONFIG, "MyStateMachine", "HappyPath")
        assert test_case is not None
        assert "StateA" in test_case

    def test_lookup_missing_state_machine(self):
        result = get_test_case(VALID_CONFIG, "NonexistentMachine", "HappyPath")
        assert result is None

    def test_lookup_missing_test_case(self):
        result = get_test_case(VALID_CONFIG, "MyStateMachine", "NonexistentCase")
        assert result is None


class TestMockStateResolution:
    def test_return_value(self):
        test_case = get_test_case(VALID_CONFIG, "MyStateMachine", "HappyPath")
        result = resolve_mock_state(test_case, "StateA")
        assert result is not None
        assert result.is_return
        assert not result.is_throw
        assert result.return_value == {"result": "success"}

    def test_throw_error(self):
        test_case = get_test_case(VALID_CONFIG, "MyStateMachine", "ErrorPath")
        result = resolve_mock_state(test_case, "StateB")
        assert result is not None
        assert result.is_throw
        assert not result.is_return
        assert result.throw_error == "CustomError"
        assert result.throw_cause == "Something failed"

    def test_state_not_in_mock(self):
        test_case = get_test_case(VALID_CONFIG, "MyStateMachine", "HappyPath")
        result = resolve_mock_state(test_case, "UnknownState")
        assert result is None


class TestHotReload:
    def test_file_change_detected(self, tmp_path, monkeypatch):
        path = tmp_path / "sfn_mock.json"
        initial_config = {
            "StateMachines": {"Machine1": {"TestCases": {"T1": {"S1": {"Return": {"v": 1}}}}}}
        }
        path.write_text(json.dumps(initial_config))
        monkeypatch.setenv("SFN_MOCK_CONFIG", str(path))

        config1 = get_mock_config()
        assert config1 is not None
        assert "Machine1" in config1["StateMachines"]

        # Ensure mtime changes (some filesystems have 1s granularity)
        time.sleep(0.05)
        updated_config = {
            "StateMachines": {"Machine2": {"TestCases": {"T2": {"S2": {"Return": {"v": 2}}}}}}
        }
        path.write_text(json.dumps(updated_config))
        # Force mtime change
        future = time.time() + 10
        os.utime(str(path), (future, future))

        config2 = get_mock_config()
        assert config2 is not None
        assert "Machine2" in config2["StateMachines"]
        assert "Machine1" not in config2["StateMachines"]


class TestExtractTestCaseFromName:
    def test_name_with_hash(self):
        clean, tc = extract_test_case_from_name("my-exec#HappyPath")
        assert clean == "my-exec"
        assert tc == "HappyPath"

    def test_name_without_hash(self):
        clean, tc = extract_test_case_from_name("my-exec")
        assert clean == "my-exec"
        assert tc is None

    def test_name_with_multiple_hashes(self):
        clean, tc = extract_test_case_from_name("a#b#c")
        assert clean == "a#b"
        assert tc == "c"
