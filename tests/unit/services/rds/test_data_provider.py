"""Unit tests for the RDS Data API provider."""

from robotocore.services.rds.data_provider import (
    _build_column_metadata,
    _convert_parameters,
    _extract_account_from_arn,
    _extract_db_identifier_from_arn,
    _extract_region_from_arn,
    _python_to_rds_field,
    _rds_param_to_python,
    _rows_to_records,
    _sqlite_type_to_rds_type,
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

    def test_zero_int(self):
        assert _python_to_rds_field(0) == {"longValue": 0}

    def test_negative_int(self):
        assert _python_to_rds_field(-42) == {"longValue": -42}

    def test_empty_string(self):
        assert _python_to_rds_field("") == {"stringValue": ""}

    def test_empty_bytes(self):
        result = _python_to_rds_field(b"")
        assert "blobValue" in result

    def test_bool_before_int(self):
        """Bool is a subclass of int in Python; ensure bool check wins."""
        result = _python_to_rds_field(True)
        assert "booleanValue" in result
        assert "longValue" not in result

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

    def test_param_blob_value(self):
        import base64

        encoded = base64.b64encode(b"\x00\x01\x02").decode()
        param = {"name": "data", "value": {"blobValue": encoded}}
        result = _rds_param_to_python(param)
        assert result == b"\x00\x01\x02"

    def test_param_empty_value(self):
        param = {"name": "x", "value": {}}
        assert _rds_param_to_python(param) is None

    def test_param_missing_value(self):
        param = {"name": "x"}
        assert _rds_param_to_python(param) is None


class TestSQLiteTypeMapping:
    """Tests for _sqlite_type_to_rds_type."""

    def test_none_maps_to_varchar(self):
        assert _sqlite_type_to_rds_type(None) == "VARCHAR"

    def test_bool_maps_to_boolean(self):
        assert _sqlite_type_to_rds_type(True) == "BOOLEAN"

    def test_int_maps_to_integer(self):
        assert _sqlite_type_to_rds_type(42) == "INTEGER"

    def test_float_maps_to_double(self):
        assert _sqlite_type_to_rds_type(3.14) == "DOUBLE"

    def test_bytes_maps_to_blob(self):
        assert _sqlite_type_to_rds_type(b"data") == "BLOB"

    def test_str_maps_to_varchar(self):
        assert _sqlite_type_to_rds_type("hello") == "VARCHAR"


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

    def test_column_metadata_type_inference(self):
        rows = [{"id": 1, "name": "Alice", "score": 9.5, "active": True}]
        metadata = _build_column_metadata(rows)
        assert metadata[0]["typeName"] == "INTEGER"
        assert metadata[1]["typeName"] == "VARCHAR"
        assert metadata[2]["typeName"] == "DOUBLE"
        assert metadata[3]["typeName"] == "BOOLEAN"

    def test_column_metadata_null_value(self):
        rows = [{"id": None}]
        metadata = _build_column_metadata(rows)
        assert metadata[0]["typeName"] == "VARCHAR"  # Fallback for NULL

    def test_column_metadata_has_all_fields(self):
        rows = [{"id": 1}]
        metadata = _build_column_metadata(rows)
        m = metadata[0]
        assert "name" in m
        assert "typeName" in m
        assert "label" in m
        assert "nullable" in m
        assert "precision" in m
        assert "scale" in m

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

    def test_rows_to_records_empty(self):
        assert _rows_to_records([]) == []

    def test_rows_to_records_with_bool(self):
        rows = [{"flag": True}]
        records = _rows_to_records(rows)
        assert records[0][0] == {"booleanValue": True}

    def test_rows_to_records_with_bytes(self):
        rows = [{"data": b"\x00\x01"}]
        records = _rows_to_records(rows)
        assert "blobValue" in records[0][0]


class TestARNParsing:
    """Tests for ARN extraction helpers."""

    def test_extract_db_identifier_from_db_arn(self):
        arn = "arn:aws:rds:us-east-1:123456789012:db:my-db"
        assert _extract_db_identifier_from_arn(arn) == "my-db"

    def test_extract_db_identifier_from_cluster_arn(self):
        arn = "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster"
        assert _extract_db_identifier_from_arn(arn) == "my-cluster"

    def test_extract_region_from_arn(self):
        arn = "arn:aws:rds:us-west-2:123456789012:db:my-db"
        assert _extract_region_from_arn(arn) == "us-west-2"

    def test_extract_account_from_arn(self):
        arn = "arn:aws:rds:us-east-1:123456789012:db:my-db"
        assert _extract_account_from_arn(arn) == "123456789012"

    def test_extract_from_short_arn(self):
        assert _extract_db_identifier_from_arn("short") is None
        assert _extract_region_from_arn("x") is None
        assert _extract_account_from_arn("x:y") is None


class TestConvertParameters:
    """Tests for _convert_parameters named/positional binding."""

    def test_positional_parameters(self):
        sql = "INSERT INTO t VALUES (?, ?)"
        params = [
            {"value": {"longValue": 1}},
            {"value": {"stringValue": "hello"}},
        ]
        result = _convert_parameters(sql, params)
        assert result == [1, "hello"]

    def test_named_parameters(self):
        sql = "INSERT INTO t VALUES (:id, :name)"
        params = [
            {"name": "id", "value": {"longValue": 1}},
            {"name": "name", "value": {"stringValue": "hello"}},
        ]
        result = _convert_parameters(sql, params)
        assert isinstance(result, dict)
        assert result["id"] == 1
        assert result["name"] == "hello"

    def test_named_parameters_with_null(self):
        sql = "INSERT INTO t VALUES (:id, :name)"
        params = [
            {"name": "id", "value": {"longValue": 1}},
            {"name": "name", "value": {"isNull": True}},
        ]
        result = _convert_parameters(sql, params)
        assert result["id"] == 1
        assert result["name"] is None


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

    def test_destroy_nonexistent_is_safe(self):
        # Should not raise
        _destroy_engine_for_instance("123456789012", "us-east-1", "never-existed")

    def test_engines_are_scoped_by_account_region(self):
        _create_engine_for_instance("111111111111", "us-east-1", "shared-name", "mysql")
        _create_engine_for_instance("222222222222", "us-east-1", "shared-name", "mysql")
        _create_engine_for_instance("111111111111", "us-west-2", "shared-name", "mysql")

        e1 = get_engine("111111111111", "us-east-1", "shared-name")
        e2 = get_engine("222222222222", "us-east-1", "shared-name")
        e3 = get_engine("111111111111", "us-west-2", "shared-name")

        assert e1 is not e2
        assert e1 is not e3
        assert e2 is not e3

        # Clean up
        _destroy_engine_for_instance("111111111111", "us-east-1", "shared-name")
        _destroy_engine_for_instance("222222222222", "us-east-1", "shared-name")
        _destroy_engine_for_instance("111111111111", "us-west-2", "shared-name")

    def test_engine_is_functional_after_creation(self):
        _create_engine_for_instance("123456789012", "us-east-1", "test-func", "mysql")
        engine = get_engine("123456789012", "us-east-1", "test-func")
        engine.execute_sql("CREATE TABLE t (x INTEGER)")
        engine.execute_sql("INSERT INTO t VALUES (?)", [42])
        rows = engine.execute_sql("SELECT x FROM t")
        assert rows[0]["x"] == 42
        _destroy_engine_for_instance("123456789012", "us-east-1", "test-func")


class TestEndToEnd:
    """End-to-end tests combining engine and data provider helpers."""

    def setup_method(self):
        _create_engine_for_instance("123456789012", "us-east-1", "e2e-db", "mysql")
        self.engine = get_engine("123456789012", "us-east-1", "e2e-db")

    def teardown_method(self):
        _destroy_engine_for_instance("123456789012", "us-east-1", "e2e-db")

    def test_create_insert_select_round_trip(self):
        self.engine.execute_sql(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)"
        )
        self.engine.execute_sql("INSERT INTO users VALUES (?, ?, ?)", [1, "Alice", True])
        rows = self.engine.execute_sql("SELECT * FROM users WHERE id = ?", [1])

        # Convert to RDS Data API format
        records = _rows_to_records(rows)
        metadata = _build_column_metadata(rows)

        assert len(records) == 1
        assert len(metadata) == 3
        assert records[0][0] == {"longValue": 1}
        assert records[0][1] == {"stringValue": "Alice"}

    def test_transaction_lifecycle(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER, val TEXT)")

        # Begin -> execute -> commit
        tx_id = self.engine.begin_transaction()
        self.engine.execute_in_transaction(tx_id, "INSERT INTO t VALUES (?, ?)", [1, "a"])
        self.engine.commit_transaction(tx_id)

        rows = self.engine.execute_sql("SELECT * FROM t")
        assert len(rows) == 1

        # Begin -> execute -> rollback
        tx_id = self.engine.begin_transaction()
        self.engine.execute_in_transaction(tx_id, "INSERT INTO t VALUES (?, ?)", [2, "b"])
        self.engine.rollback_transaction(tx_id)

        rows = self.engine.execute_sql("SELECT * FROM t")
        assert len(rows) == 1  # Still just the one from committed tx
