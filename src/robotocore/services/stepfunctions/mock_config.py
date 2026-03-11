"""StepFunctions mock configuration support.

Loads a JSON mock config file (specified by the SFN_MOCK_CONFIG env var) that provides
deterministic, pre-defined execution results for testing. Compatible with LocalStack's
mock config format.

Config format:
{
  "StateMachines": {
    "MyStateMachine": {
      "TestCases": {
        "HappyPath": {
          "StateA": {"Return": {"result": "success"}},
          "StateB": {"Return": {"count": 42}}
        },
        "ErrorPath": {
          "StateA": {"Return": {"result": "success"}},
          "StateB": {"Throw": {"Error": "CustomError", "Cause": "Something failed"}}
        }
      }
    }
  }
}

Test case selection: pass '#TestCaseName' suffix in execution name or via X-SFN-Mock-Config header.
"""

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# Module-level cached config
_mock_config: dict | None = None
_config_path: str | None = None
_config_mtime: float = 0.0


class MockStateResult:
    """Result of a mock state lookup — either a Return value or a Throw error."""

    def __init__(
        self,
        *,
        return_value: Any = None,
        throw_error: str | None = None,
        throw_cause: str | None = None,
        is_return: bool = True,
    ):
        self.return_value = return_value
        self.throw_error = throw_error
        self.throw_cause = throw_cause
        self.is_return = is_return

    @property
    def is_throw(self) -> bool:
        return not self.is_return


def load_mock_config(path: str | None = None) -> dict | None:
    """Load and return mock config from a JSON file.

    Args:
        path: Path to the JSON config file. If None, reads from SFN_MOCK_CONFIG env var.

    Returns:
        Parsed config dict, or None if no config file is specified/found.

    Raises:
        ValueError: If the file contains invalid JSON (with a helpful message).
    """
    if path is None:
        path = os.environ.get("SFN_MOCK_CONFIG")

    if not path:
        return None

    if not os.path.exists(path):
        logger.warning("SFN_MOCK_CONFIG file not found: %s", path)
        return None

    try:
        with open(path) as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in SFN_MOCK_CONFIG file '{path}': {e}") from e

    _validate_config(config)
    return config


def _validate_config(config: dict) -> None:
    """Basic structural validation of the mock config."""
    if not isinstance(config, dict):
        raise ValueError("Mock config must be a JSON object")
    if "StateMachines" not in config:
        raise ValueError("Mock config must contain a 'StateMachines' key")
    sms = config["StateMachines"]
    if not isinstance(sms, dict):
        raise ValueError("'StateMachines' must be a JSON object")


def get_mock_config() -> dict | None:
    """Get the current mock config, reloading if the file has changed.

    Uses mtime-based hot-reload: checks the file modification time on each call
    and reloads if it has changed since the last read.
    """
    global _mock_config, _config_path, _config_mtime

    path = os.environ.get("SFN_MOCK_CONFIG")
    if not path:
        _mock_config = None
        _config_path = None
        return None

    # Check if file has changed
    try:
        current_mtime = os.path.getmtime(path)
    except OSError:
        logger.warning("SFN_MOCK_CONFIG file not accessible: %s", path)
        return _mock_config  # Return cached if file temporarily unavailable

    if path != _config_path or current_mtime != _config_mtime:
        try:
            _mock_config = load_mock_config(path)
            _config_path = path
            _config_mtime = current_mtime
            logger.info("Loaded SFN mock config from %s", path)
        except (ValueError, OSError) as e:
            logger.error("Failed to reload SFN mock config: %s", e)
            # Keep using the old config if reload fails

    return _mock_config


def reset_mock_config() -> None:
    """Reset the cached mock config. Useful for testing."""
    global _mock_config, _config_path, _config_mtime
    _mock_config = None
    _config_path = None
    _config_mtime = 0.0


def get_state_machine_names(config: dict) -> list[str]:
    """Extract state machine names from the config."""
    return list(config.get("StateMachines", {}).keys())


def get_test_case_names(config: dict, state_machine_name: str) -> list[str]:
    """Extract test case names for a given state machine."""
    sm = config.get("StateMachines", {}).get(state_machine_name, {})
    return list(sm.get("TestCases", {}).keys())


def get_test_case(config: dict, state_machine_name: str, test_case_name: str) -> dict | None:
    """Look up a test case by state machine name and test case name.

    Returns:
        Dict mapping state names to their mock definitions, or None if not found.
    """
    sm = config.get("StateMachines", {}).get(state_machine_name)
    if sm is None:
        return None
    return sm.get("TestCases", {}).get(test_case_name)


def resolve_mock_state(test_case: dict, state_name: str) -> MockStateResult | None:
    """Resolve mock config for a specific state within a test case.

    Args:
        test_case: The test case dict (state_name -> mock definition).
        state_name: The name of the state to look up.

    Returns:
        MockStateResult with Return or Throw data, or None if the state
        is not in the mock config (should execute normally).
    """
    state_mock = test_case.get(state_name)
    if state_mock is None:
        return None

    if "Return" in state_mock:
        return MockStateResult(return_value=state_mock["Return"], is_return=True)
    elif "Throw" in state_mock:
        throw = state_mock["Throw"]
        return MockStateResult(
            throw_error=throw.get("Error", "MockError"),
            throw_cause=throw.get("Cause", ""),
            is_return=False,
        )

    return None


def extract_test_case_from_name(execution_name: str) -> tuple[str, str | None]:
    """Extract test case name from execution name.

    If the execution name contains '#', everything after the last '#' is the
    test case name.

    Args:
        execution_name: The execution name, possibly with '#TestCase' suffix.

    Returns:
        Tuple of (clean_name, test_case_name). test_case_name is None if no
        '#' suffix was present.
    """
    if "#" in execution_name:
        parts = execution_name.rsplit("#", 1)
        return parts[0], parts[1]
    return execution_name, None
