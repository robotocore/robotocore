"""Unit tests for the recursive botocore shape validator."""

import botocore.session

from src.robotocore.testing.shape_validator import (
    ShapeValidationResult,
    ShapeViolation,
    validate_operation_response,
    validate_shape,
)


class TestValidateShape:
    """Test the core recursive shape walker."""

    def _get_output_shape(self, service: str, operation: str):
        session = botocore.session.get_session()
        model = session.get_service_model(service)
        op_model = model.operation_model(operation)
        return op_model.output_shape

    def test_valid_list_buckets_response(self):
        """A correct ListBuckets response produces no errors."""
        shape = self._get_output_shape("s3", "ListBuckets")
        response = {
            "Buckets": [{"Name": "my-bucket", "CreationDate": "2024-01-01T00:00:00Z"}],
            "Owner": {"DisplayName": "me", "ID": "abc123"},
        }
        violations = validate_shape(shape, response, check_optional=False)
        errors = [v for v in violations if v.severity == "error"]
        assert len(errors) == 0

    def test_type_mismatch_caught(self):
        """A wrong type is reported as an error."""
        shape = self._get_output_shape("s3", "ListBuckets")
        response = {
            "Buckets": "not-a-list",  # Should be a list
        }
        violations = validate_shape(shape, response, check_optional=False)
        errors = [v for v in violations if v.severity == "error"]
        assert any(v.issue == "type_mismatch" and "Buckets" in v.path for v in errors)

    def test_extra_key_reported_as_info(self):
        """Keys not in the model are reported as info (not errors)."""
        shape = self._get_output_shape("s3", "ListBuckets")
        response = {
            "Buckets": [],
            "SomethingExtra": "unexpected",
        }
        violations = validate_shape(shape, response, check_optional=False)
        info = [v for v in violations if v.severity == "info"]
        assert any(v.issue == "extra_key" and "SomethingExtra" in v.path for v in info)

    def test_missing_optional_reported_as_warning(self):
        """Missing optional keys are warnings when check_optional=True."""
        shape = self._get_output_shape("s3", "ListBuckets")
        response = {}  # Missing Buckets and Owner (both optional in botocore)
        violations = validate_shape(shape, response, check_optional=True)
        warnings = [v for v in violations if v.severity == "warning"]
        assert len(warnings) > 0

    def test_missing_optional_suppressed(self):
        """Missing optional keys are not reported when check_optional=False."""
        shape = self._get_output_shape("s3", "ListBuckets")
        response = {}
        violations = validate_shape(shape, response, check_optional=False)
        warnings = [v for v in violations if v.severity == "warning"]
        assert len(warnings) == 0

    def test_nested_structure_validation(self):
        """Nested structures are validated recursively."""
        shape = self._get_output_shape("dynamodb", "DescribeTable")
        response = {
            "Table": {
                "TableName": "test",
                "TableStatus": "ACTIVE",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "ItemCount": 0,
                "TableSizeBytes": 0,
                "CreationDateTime": "2024-01-01T00:00:00Z",
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                },
                "TableArn": "arn:aws:dynamodb:us-east-1:123456789012:table/test",
            }
        }
        violations = validate_shape(shape, response, check_optional=False)
        errors = [v for v in violations if v.severity == "error"]
        assert len(errors) == 0

    def test_list_element_validation(self):
        """List elements are validated against the member shape."""
        shape = self._get_output_shape("sqs", "ListQueues")
        response = {
            "QueueUrls": [123],  # Should be strings
        }
        violations = validate_shape(shape, response, check_optional=False)
        errors = [v for v in violations if v.severity == "error"]
        assert any(v.issue == "type_mismatch" for v in errors)

    def test_empty_list_no_errors(self):
        """An empty list is valid (no elements to validate)."""
        shape = self._get_output_shape("sqs", "ListQueues")
        response = {
            "QueueUrls": [],
        }
        violations = validate_shape(shape, response, check_optional=False)
        errors = [v for v in violations if v.severity == "error"]
        assert len(errors) == 0

    def test_none_value_no_crash(self):
        """None values don't crash the validator."""
        shape = self._get_output_shape("s3", "ListBuckets")
        response = {
            "Buckets": None,
        }
        violations = validate_shape(shape, response, check_optional=False)
        errors = [v for v in violations if v.severity == "error"]
        # None where a list is expected is a type mismatch
        assert any(v.issue == "type_mismatch" for v in errors)


class TestValidateOperationResponse:
    """Test the high-level validate_operation_response function."""

    def test_valid_response(self):
        """A valid response passes."""
        response = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "Buckets": [],
        }
        result = validate_operation_response("s3", "ListBuckets", response, check_optional=False)
        assert result.passed
        assert result.service == "s3"
        assert result.operation == "ListBuckets"

    def test_response_metadata_stripped(self):
        """ResponseMetadata is not validated against the shape."""
        response = {
            "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "abc"},
            "Buckets": [],
        }
        result = validate_operation_response("s3", "ListBuckets", response, check_optional=False)
        # ResponseMetadata should not show up as extra_key
        extra = [v for v in result.violations if v.issue == "extra_key"]
        assert not any("ResponseMetadata" in v.path for v in extra)

    def test_invalid_service_skipped(self):
        """Unknown service is skipped, not crashed."""
        result = validate_operation_response(
            "nonexistent-service", "FakeOp", {}, check_optional=False
        )
        assert result.skipped
        assert "cannot load model" in result.skip_reason

    def test_summary_output(self):
        """Summary string is human-readable."""
        result = ShapeValidationResult(service="s3", operation="ListBuckets")
        assert "PASS" in result.summary()

        result.violations.append(
            ShapeViolation(
                path="Foo",
                issue="type_mismatch",
                expected="string",
                actual="integer",
                severity="error",
            )
        )
        assert "FAIL" in result.summary()

    def test_skipped_summary(self):
        """Skipped result shows reason in summary."""
        result = ShapeValidationResult(
            service="s3",
            operation="ListBuckets",
            skipped=True,
            skip_reason="no params",
        )
        assert "SKIP" in result.summary()


class TestShapeViolationStr:
    """Test ShapeViolation string representation."""

    def test_missing_required(self):
        v = ShapeViolation("Foo.Bar", "missing_required", "string", None, "error")
        assert "ERROR" in str(v)
        assert "Foo.Bar" in str(v)

    def test_type_mismatch(self):
        v = ShapeViolation("Foo", "type_mismatch", "string", "integer", "error")
        assert "type mismatch" in str(v)

    def test_extra_key(self):
        v = ShapeViolation("Baz", "extra_key", "(not in model)", "string", "info")
        assert "INFO" in str(v)
