"""Tests for chaos engineering bug fixes.

Validates fixes for real bugs found during code audit:
- status_code=0 was silently overridden due to `0 or 500` falsy check
- Invalid regex in operation crashed with raw re.error instead of ValueError
"""

import pytest

from robotocore.chaos.fault_rules import FaultRule


class TestStatusCodeZeroPreserved:
    """Fixed: status_code=0 is now preserved instead of being treated as falsy."""

    def test_explicit_status_code_zero_is_preserved(self):
        rule = FaultRule(error_code="ThrottlingException", status_code=0)
        assert rule.status_code == 0

    def test_from_dict_status_code_zero_preserved(self):
        data = {"error_code": "ThrottlingException", "status_code": 0}
        rule = FaultRule.from_dict(data)
        assert rule.status_code == 0

    def test_roundtrip_status_code_zero(self):
        rule = FaultRule(error_code="ThrottlingException", status_code=0)
        d = rule.to_dict()
        assert d["status_code"] == 0
        restored = FaultRule.from_dict(d)
        assert restored.status_code == 0


class TestInvalidRegexRaisesValueError:
    """Fixed: invalid regex now raises ValueError instead of raw re.error."""

    def test_invalid_regex_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid regex"):
            FaultRule(operation="[unclosed")

    def test_from_dict_invalid_regex_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid regex"):
            FaultRule.from_dict({"operation": "(unclosed"})
