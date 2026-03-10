"""Unit tests for the RDS Data API provider."""

from robotocore.services.rds.data_provider import (
    _build_column_metadata,
    _python_to_rds_field,
    _rds_param_to_python,
    _rows_to_records,
)
from robotocore.services.rds.provider import (
    _create_engine_for_instance,
    _destroy_engine_for_instance,
    get_engine,
)


class TestTypeConversion:
    """Tests for Python <-> RDS Data API type mapping."""

    def test_int_to_long_value(self):
        assert _python_to_rds_field(42) == {"longValue": 42}

    def test_float_to_double_value(self):
        assert _python_to_rds_field(3.14) == {"doubleValue": 3.14}

    def test_str_to_string_value(self):
        assert _python_to_rds_field("hello") == {"stringValue": "hello"}

    def test_none_to_is_null(self):
        assert _python_to_rds_field(None) == {"isNull": True}

    def test_bool_to_boolean_value(self):
        assert _python_to_rds_field(True) == {"booleanValue": True}
        assert _python_to_rds_field(False) == {"booleanValue": False}

    def test_bytes_to_blob_value(self):
        result = _python_to_rds_field(b"\x00\x01\x02")
        assert "blobValue" in result

    def test_param_string_value(self):
        param = {"name": "name", "value": {"stringValue": "Alice"}}
        assert _rds_param_to_python(param) == "Alice"

    def test_param_long_value(self):
        param = {"name": "id", "value": {"longValue": 42}}
        assert _rds_param_to_python(param) == 42

    def test_param_is_null(self):
        param = {"name": "x", "value": {"isNull": True}}
        assert _rds_param_to_python(param) is None

    def test_param_boolean_value(self):
        param = {"name": "flag", "value": {"booleanValue": True}}
        assert _rds_param_to_python(param) is True

    def test_param_double_value(self):
        param = {"name": "price", "value": {"doubleValue": 9.99}}
        assert _rds_param_to_python(param) == 9.99


class TestResultFormatting:
    """Tests for result formatting to RDS Data API shape."""

    def test_column_metadata_from_rows(self):
        rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        metadata = _build_column_metadata(rows)
        assert len(metadata) == 2
        assert metadata[0]["name"] == "id"
        assert metadata[1]["name"] == "name"
        assert "typeName" in metadata[0]
        assert "label" in metadata[0]

    def test_column_metadata_empty(self):
        assert _build_column_metadata([]) == []

    def test_column_metadata_dml(self):
        rows = [{"rowsAffected": 1}]
        assert _build_column_metadata(rows) == []

    def test_rows_to_records(self):
        rows = [
            {"id": 1, "name": "Alice", "score": 9.5},
            {"id": 2, "name": "Bob", "score": None},
        ]
        records = _rows_to_records(rows)
        assert len(records) == 2
        assert records[0][0] == {"longValue": 1}
        assert records[0][1] == {"stringValue": "Alice"}
        assert records[0][2] == {"doubleValue": 9.5}
        assert records[1][2] == {"isNull": True}

    def test_rows_to_records_skips_dml(self):
        rows = [{"rowsAffected": 3}]
        records = _rows_to_records(rows)
        assert records == []


class TestEngineRegistration:
    """Tests for engine lifecycle via provider module."""

    def test_create_and_get_engine(self):
        _create_engine_for_instance("123456789012", "us-east-1", "test-db-reg", "mysql")
        engine = get_engine("123456789012", "us-east-1", "test-db-reg")
        assert engine is not None
        # Clean up
        _destroy_engine_for_instance("123456789012", "us-east-1", "test-db-reg")

    def test_destroy_engine(self):
        _create_engine_for_instance("123456789012", "us-east-1", "test-db-destroy", "mysql")
        _destroy_engine_for_instance("123456789012", "us-east-1", "test-db-destroy")
        engine = get_engine("123456789012", "us-east-1", "test-db-destroy")
        assert engine is None

    def test_get_nonexistent_engine(self):
        engine = get_engine("123456789012", "us-east-1", "nonexistent-db")
        assert engine is None
