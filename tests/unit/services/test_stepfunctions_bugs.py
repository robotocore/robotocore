"""Tests for correctness bugs in the Step Functions provider."""

from robotocore.services.stepfunctions.asl import _is_ddb_typed


class TestIsDdbTypedBug:
    def test_is_ddb_typed_checks_all_values(self):
        """_is_ddb_typed should inspect all values, not just the first."""
        # First value has 2 keys so it's not recognized as typed.
        # Second value {"S": "Alice"} IS typed. The function should return True.
        item = {"multi_attr": {"S": "val", "extra": "x"}, "name": {"S": "Alice"}}
        result = _is_ddb_typed(item)
        assert result is True
