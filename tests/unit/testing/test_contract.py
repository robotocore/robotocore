"""Unit tests for the contract testing framework.

Tests the Contract, AWSContract, and ValidationResult classes without
requiring a running server or real AWS access.
"""

from robotocore.testing.contract import (
    AWSContract,
    Contract,
    ValidationResult,
    _botocore_type_name,
    _error_format_for_protocol,
    _expected_headers_for_protocol,
    _python_type_name,
    detect_response_format,
    load_contracts,
    save_contracts,
    validate_json_structure,
    validate_xml_structure,
)

# --- Contract dataclass ---


class TestContract:
    def test_to_dict_roundtrip(self):
        contract = Contract(
            service="s3",
            operation="ListBuckets",
            status_code=200,
            response_keys={"Buckets", "Owner"},
            header_keys={"content-type", "x-amz-request-id"},
            error_format=None,
            metadata_keys={"RequestId", "HTTPStatusCode"},
            timestamp="2026-03-11T00:00:00+00:00",
            protocol="rest-xml",
            key_types={"Buckets": "list", "Owner": "structure"},
        )
        d = contract.to_dict()
        restored = Contract.from_dict(d)
        assert restored.service == "s3"
        assert restored.operation == "ListBuckets"
        assert restored.response_keys == {"Buckets", "Owner"}
        assert restored.header_keys == {"content-type", "x-amz-request-id"}
        assert restored.protocol == "rest-xml"
        assert restored.key_types == {"Buckets": "list", "Owner": "structure"}

    def test_to_dict_sorts_sets(self):
        contract = Contract(
            service="dynamodb",
            operation="ListTables",
            status_code=200,
            response_keys={"TableNames", "LastEvaluatedTableName"},
            header_keys={"x-amzn-requestid", "content-type"},
            error_format=None,
            metadata_keys={"RequestId"},
            timestamp="2026-03-11T00:00:00+00:00",
            protocol="json",
            key_types={},
        )
        d = contract.to_dict()
        assert d["response_keys"] == ["LastEvaluatedTableName", "TableNames"]
        assert d["header_keys"] == ["content-type", "x-amzn-requestid"]

    def test_from_dict_with_error_format(self):
        data = {
            "service": "s3",
            "operation": "GetObject",
            "status_code": 404,
            "response_keys": [],
            "header_keys": ["content-type"],
            "error_format": {"Code": "str", "Message": "str"},
            "metadata_keys": ["RequestId"],
            "timestamp": "2026-03-11T00:00:00+00:00",
            "protocol": "rest-xml",
            "key_types": {},
        }
        contract = Contract.from_dict(data)
        assert contract.error_format == {"Code": "str", "Message": "str"}

    def test_from_dict_missing_optional_fields(self):
        data = {
            "service": "sts",
            "operation": "GetCallerIdentity",
            "status_code": 200,
            "response_keys": ["Account", "Arn", "UserId"],
            "header_keys": [],
            "metadata_keys": ["RequestId"],
            "timestamp": "2026-03-11T00:00:00+00:00",
        }
        contract = Contract.from_dict(data)
        assert contract.protocol == ""
        assert contract.key_types == {}
        assert contract.error_format is None


# --- AWSContract.record ---


class TestAWSContractRecord:
    def test_record_basic_response(self):
        response = {
            "Buckets": [{"Name": "my-bucket"}],
            "Owner": {"ID": "abc123"},
            "ResponseMetadata": {
                "RequestId": "abc-123",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {"content-type": "application/xml"},
            },
        }
        headers = {"Content-Type": "application/xml", "x-amz-request-id": "abc-123"}

        contract = AWSContract.record("s3", "ListBuckets", response, headers, protocol="rest-xml")

        assert contract.service == "s3"
        assert contract.operation == "ListBuckets"
        assert contract.response_keys == {"Buckets", "Owner"}
        assert contract.status_code == 200
        assert "content-type" in contract.header_keys
        assert "x-amz-request-id" in contract.header_keys
        assert contract.protocol == "rest-xml"
        assert contract.key_types == {"Buckets": "list", "Owner": "structure"}

    def test_record_excludes_response_metadata(self):
        response = {
            "TableNames": ["table1"],
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        headers = {}

        contract = AWSContract.record("dynamodb", "ListTables", response, headers)
        assert "ResponseMetadata" not in contract.response_keys
        assert contract.response_keys == {"TableNames"}

    def test_record_captures_metadata_keys(self):
        response = {
            "ResponseMetadata": {
                "RequestId": "abc",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {},
                "RetryAttempts": 0,
            },
        }
        contract = AWSContract.record("sts", "GetCallerIdentity", response, {})
        assert "RequestId" in contract.metadata_keys
        assert "HTTPStatusCode" in contract.metadata_keys
        assert "RetryAttempts" in contract.metadata_keys

    def test_record_error_response(self):
        response = {
            "Error": {"Code": "NoSuchBucket", "Message": "The bucket does not exist"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }
        headers = {}

        contract = AWSContract.record("s3", "GetObject", response, headers)
        assert contract.error_format is not None
        assert "Code" in contract.error_format
        assert "Message" in contract.error_format

    def test_record_normalizes_header_keys(self):
        headers = {"Content-Type": "application/json", "X-Amzn-RequestId": "abc"}
        response = {"ResponseMetadata": {"HTTPStatusCode": 200}}

        contract = AWSContract.record("dynamodb", "ListTables", response, headers)
        assert "content-type" in contract.header_keys
        assert "x-amzn-requestid" in contract.header_keys

    def test_record_key_types(self):
        response = {
            "Count": 5,
            "Items": [{"id": "1"}],
            "IsActive": True,
            "Name": "test",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        contract = AWSContract.record("test", "TestOp", response, {})
        assert contract.key_types["Count"] == "integer"
        assert contract.key_types["Items"] == "list"
        assert contract.key_types["IsActive"] == "boolean"
        assert contract.key_types["Name"] == "string"


# --- AWSContract.validate ---


class TestAWSContractValidate:
    def _make_contract(self, **overrides) -> Contract:
        defaults = {
            "service": "dynamodb",
            "operation": "ListTables",
            "status_code": 200,
            "response_keys": {"TableNames"},
            "header_keys": {"content-type", "x-amzn-requestid"},
            "error_format": None,
            "metadata_keys": {"RequestId", "HTTPStatusCode"},
            "timestamp": "2026-03-11T00:00:00+00:00",
            "protocol": "json",
            "key_types": {"TableNames": "list"},
        }
        defaults.update(overrides)
        return Contract(**defaults)

    def test_validate_pass(self):
        contract = self._make_contract()
        response = {
            "TableNames": [],
            "ResponseMetadata": {
                "RequestId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "HTTPStatusCode": 200,
            },
        }
        headers = {
            "content-type": "application/x-amz-json-1.0",
            "x-amzn-requestid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        }

        result = AWSContract.validate("dynamodb", "ListTables", response, headers, contract)
        assert result.passed is True
        assert result.missing_keys == []
        assert result.extra_keys == []
        assert result.wrong_types == []

    def test_validate_missing_keys(self):
        contract = self._make_contract(
            response_keys={"TableNames", "LastEvaluatedTableName"},
            key_types={"TableNames": "list", "LastEvaluatedTableName": "string"},
        )
        response = {
            "TableNames": [],
            "ResponseMetadata": {"RequestId": "abc", "HTTPStatusCode": 200},
        }
        headers = {"content-type": "application/json", "x-amzn-requestid": "abc"}

        result = AWSContract.validate("dynamodb", "ListTables", response, headers, contract)
        assert result.passed is False
        assert "LastEvaluatedTableName" in result.missing_keys

    def test_validate_extra_keys(self):
        contract = self._make_contract(response_keys={"TableNames"})
        response = {
            "TableNames": [],
            "ExtraField": "unexpected",
            "ResponseMetadata": {"RequestId": "abc", "HTTPStatusCode": 200},
        }
        headers = {"content-type": "application/json", "x-amzn-requestid": "abc"}

        result = AWSContract.validate("dynamodb", "ListTables", response, headers, contract)
        assert result.passed is False
        assert "ExtraField" in result.extra_keys

    def test_validate_wrong_types(self):
        contract = self._make_contract(key_types={"TableNames": "list"})
        response = {
            "TableNames": "not-a-list",
            "ResponseMetadata": {"RequestId": "abc", "HTTPStatusCode": 200},
        }
        headers = {"content-type": "application/json", "x-amzn-requestid": "abc"}

        result = AWSContract.validate("dynamodb", "ListTables", response, headers, contract)
        assert result.passed is False
        assert len(result.wrong_types) == 1
        assert result.wrong_types[0] == ("TableNames", "list", "string")

    def test_validate_missing_headers(self):
        contract = self._make_contract()
        response = {
            "TableNames": [],
            "ResponseMetadata": {"RequestId": "abc", "HTTPStatusCode": 200},
        }
        headers = {"content-type": "application/json"}

        result = AWSContract.validate("dynamodb", "ListTables", response, headers, contract)
        assert result.passed is False
        assert "x-amzn-requestid" in result.missing_headers

    def test_validate_missing_metadata_key(self):
        contract = self._make_contract()
        response = {
            "TableNames": [],
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        headers = {"content-type": "application/json", "x-amzn-requestid": "abc"}

        result = AWSContract.validate("dynamodb", "ListTables", response, headers, contract)
        assert result.passed is False
        assert any("RequestId" in m for m in result.format_mismatches)

    def test_validate_error_format(self):
        contract = self._make_contract(
            error_format={"Code": "str", "Message": "str"},
            response_keys=set(),
            key_types={},
        )
        response = {
            "Error": {"Code": "ResourceNotFoundException"},
            "ResponseMetadata": {"RequestId": "abc", "HTTPStatusCode": 400},
        }
        headers = {"content-type": "application/json", "x-amzn-requestid": "abc"}

        result = AWSContract.validate("dynamodb", "ListTables", response, headers, contract)
        assert result.passed is False
        assert any("Message" in m for m in result.format_mismatches)

    def test_validate_error_format_missing_error(self):
        contract = self._make_contract(
            error_format={"Code": "str", "Message": "str"},
            response_keys=set(),
            key_types={},
        )
        response = {
            "TableNames": [],
            "ResponseMetadata": {"RequestId": "abc", "HTTPStatusCode": 200},
        }
        headers = {"content-type": "application/json", "x-amzn-requestid": "abc"}

        result = AWSContract.validate("dynamodb", "ListTables", response, headers, contract)
        assert result.passed is False
        assert any("Expected error" in m for m in result.format_mismatches)

    def test_validate_request_id_format_json_uuid(self):
        contract = self._make_contract(protocol="json")
        response = {
            "TableNames": [],
            "ResponseMetadata": {"RequestId": "abc", "HTTPStatusCode": 200},
        }
        # Invalid UUID format
        headers = {
            "content-type": "application/json",
            "x-amzn-requestid": "not-a-uuid",
        }

        result = AWSContract.validate("dynamodb", "ListTables", response, headers, contract)
        assert any("UUID" in m for m in result.format_mismatches)

    def test_validate_request_id_format_json_valid(self):
        contract = self._make_contract(protocol="json")
        response = {
            "TableNames": [],
            "ResponseMetadata": {"RequestId": "abc", "HTTPStatusCode": 200},
        }
        headers = {
            "content-type": "application/json",
            "x-amzn-requestid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        }

        result = AWSContract.validate("dynamodb", "ListTables", response, headers, contract)
        assert not any("UUID" in m for m in result.format_mismatches)


# --- AWSContract.from_botocore ---


class TestAWSContractFromBotocore:
    def test_s3_list_buckets(self):
        contract = AWSContract.from_botocore("s3", "ListBuckets")
        assert contract.service == "s3"
        assert contract.operation == "ListBuckets"
        assert contract.protocol == "rest-xml"
        assert "Buckets" in contract.response_keys
        assert "Owner" in contract.response_keys
        assert contract.key_types["Buckets"] == "list"
        assert contract.key_types["Owner"] == "structure"

    def test_dynamodb_list_tables(self):
        contract = AWSContract.from_botocore("dynamodb", "ListTables")
        assert contract.protocol == "json"
        assert "TableNames" in contract.response_keys
        assert contract.key_types["TableNames"] == "list"

    def test_sts_get_caller_identity(self):
        contract = AWSContract.from_botocore("sts", "GetCallerIdentity")
        assert contract.protocol == "query"
        assert "Account" in contract.response_keys
        assert "Arn" in contract.response_keys
        assert "UserId" in contract.response_keys

    def test_lambda_list_functions(self):
        contract = AWSContract.from_botocore("lambda", "ListFunctions")
        assert contract.protocol == "rest-json"
        assert "Functions" in contract.response_keys

    def test_error_format_for_json_protocol(self):
        contract = AWSContract.from_botocore("dynamodb", "ListTables")
        assert contract.error_format == {"Code": "str", "Message": "str"}

    def test_error_format_for_query_protocol(self):
        contract = AWSContract.from_botocore("iam", "ListUsers")
        assert contract.error_format == {"Code": "str", "Message": "str", "Type": "str"}

    def test_headers_for_json_protocol(self):
        contract = AWSContract.from_botocore("dynamodb", "ListTables")
        assert "x-amzn-requestid" in contract.header_keys
        assert "content-type" in contract.header_keys

    def test_headers_for_rest_xml_protocol(self):
        contract = AWSContract.from_botocore("s3", "ListBuckets")
        assert "x-amz-request-id" in contract.header_keys

    def test_metadata_keys(self):
        contract = AWSContract.from_botocore("s3", "ListBuckets")
        assert "RequestId" in contract.metadata_keys
        assert "HTTPStatusCode" in contract.metadata_keys


# --- ValidationResult ---


class TestValidationResult:
    def test_to_dict(self):
        result = ValidationResult(
            passed=False,
            missing_keys=["Buckets"],
            extra_keys=["Unexpected"],
            wrong_types=[("Count", "integer", "string")],
            missing_headers=["x-amz-request-id"],
            format_mismatches=["Invalid XML"],
        )
        d = result.to_dict()
        assert d["passed"] is False
        assert d["missing_keys"] == ["Buckets"]
        assert d["extra_keys"] == ["Unexpected"]
        assert d["wrong_types"] == [{"key": "Count", "expected": "integer", "actual": "string"}]
        assert d["missing_headers"] == ["x-amz-request-id"]
        assert d["format_mismatches"] == ["Invalid XML"]

    def test_to_dict_passing(self):
        result = ValidationResult(passed=True)
        d = result.to_dict()
        assert d["passed"] is True
        assert d["missing_keys"] == []
        assert d["wrong_types"] == []


# --- Wire format helpers ---


class TestDetectResponseFormat:
    def test_json_content_type(self):
        assert detect_response_format("application/x-amz-json-1.0", "{}") == "json"

    def test_xml_content_type(self):
        assert detect_response_format("application/xml", "<Root/>") == "xml"

    def test_json_body_fallback(self):
        assert detect_response_format("text/plain", '{"key": "value"}') == "json"

    def test_xml_body_fallback(self):
        assert detect_response_format("text/plain", "<?xml version='1.0'?><Root/>") == "xml"

    def test_xml_tag_fallback(self):
        assert detect_response_format("text/plain", "<ListBucketsResult/>") == "xml"

    def test_unknown(self):
        assert detect_response_format("text/plain", "just plain text") == "unknown"

    def test_bytes_body(self):
        assert detect_response_format("text/plain", b'{"key": 1}') == "json"


class TestValidateXMLStructure:
    def test_valid_xml(self):
        xml = "<ListBucketsResult><Buckets/></ListBucketsResult>"
        assert validate_xml_structure(xml) == []

    def test_invalid_xml(self):
        result = validate_xml_structure("<unclosed>")
        assert len(result) == 1
        assert "Invalid XML" in result[0]

    def test_expected_root(self):
        xml = "<ListBucketsResult><Buckets/></ListBucketsResult>"
        assert validate_xml_structure(xml, expected_root="ListBucketsResult") == []

    def test_wrong_root(self):
        xml = "<OtherResult><Data/></OtherResult>"
        result = validate_xml_structure(xml, expected_root="ListBucketsResult")
        assert len(result) == 1
        assert "Expected root" in result[0]

    def test_bytes_body(self):
        xml = b"<Root/>"
        assert validate_xml_structure(xml) == []


class TestValidateJSONStructure:
    def test_valid_json(self):
        assert validate_json_structure('{"TableNames": []}') == []

    def test_invalid_json(self):
        result = validate_json_structure("{invalid}")
        assert len(result) == 1
        assert "Invalid JSON" in result[0]

    def test_bytes_body(self):
        assert validate_json_structure(b'{"key": 1}') == []


# --- Type mapping helpers ---


class TestTypeHelpers:
    def test_python_type_name_string(self):
        assert _python_type_name("hello") == "string"

    def test_python_type_name_int(self):
        assert _python_type_name(42) == "integer"

    def test_python_type_name_float(self):
        assert _python_type_name(3.14) == "float"

    def test_python_type_name_bool(self):
        assert _python_type_name(True) == "boolean"

    def test_python_type_name_list(self):
        assert _python_type_name([1, 2]) == "list"

    def test_python_type_name_dict(self):
        assert _python_type_name({"key": "val"}) == "structure"

    def test_python_type_name_none(self):
        assert _python_type_name(None) == "null"

    def test_botocore_type_name_timestamp(self):
        assert _botocore_type_name("timestamp") == "string"

    def test_botocore_type_name_long(self):
        assert _botocore_type_name("long") == "integer"

    def test_botocore_type_name_map(self):
        assert _botocore_type_name("map") == "structure"


# --- Protocol helpers ---


class TestProtocolHelpers:
    def test_json_headers(self):
        headers = _expected_headers_for_protocol("json")
        assert "x-amzn-requestid" in headers
        assert "content-type" in headers

    def test_rest_json_headers(self):
        headers = _expected_headers_for_protocol("rest-json")
        assert "x-amzn-requestid" in headers

    def test_rest_xml_headers(self):
        headers = _expected_headers_for_protocol("rest-xml")
        assert "x-amz-request-id" in headers

    def test_query_headers(self):
        headers = _expected_headers_for_protocol("query")
        assert "content-type" in headers
        # Query protocol returns request ID in body, not header
        assert "x-amzn-requestid" not in headers

    def test_json_error_format(self):
        fmt = _error_format_for_protocol("json")
        assert "Code" in fmt
        assert "Message" in fmt

    def test_query_error_format(self):
        fmt = _error_format_for_protocol("query")
        assert "Code" in fmt
        assert "Message" in fmt
        assert "Type" in fmt


# --- Persistence ---


class TestPersistence:
    def test_save_and_load(self, tmp_path):
        contracts = [
            Contract(
                service="s3",
                operation="ListBuckets",
                status_code=200,
                response_keys={"Buckets", "Owner"},
                header_keys={"content-type"},
                error_format=None,
                metadata_keys={"RequestId"},
                timestamp="2026-03-11T00:00:00+00:00",
                protocol="rest-xml",
                key_types={"Buckets": "list"},
            ),
        ]
        save_contracts(tmp_path, "s3", contracts)

        loaded = load_contracts(tmp_path)
        assert "s3" in loaded
        assert len(loaded["s3"]) == 1
        c = loaded["s3"][0]
        assert c.service == "s3"
        assert c.operation == "ListBuckets"
        assert c.response_keys == {"Buckets", "Owner"}

    def test_load_empty_dir(self, tmp_path):
        loaded = load_contracts(tmp_path)
        assert loaded == {}

    def test_load_nonexistent_dir(self, tmp_path):
        loaded = load_contracts(tmp_path / "nonexistent")
        assert loaded == {}

    def test_save_creates_dir(self, tmp_path):
        target = tmp_path / "sub" / "dir"
        save_contracts(target, "test", [])
        assert target.exists()
