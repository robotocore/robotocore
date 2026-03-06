"""Tests for botocore-based service info lookup."""

from robotocore.protocols.service_info import get_service_operations, get_service_protocol


class TestGetServiceProtocol:
    def test_query_services(self):
        assert get_service_protocol("sts") == "query"
        assert get_service_protocol("iam") == "query"
        assert get_service_protocol("sns") == "query"

    def test_json_services(self):
        assert get_service_protocol("dynamodb") == "json"
        assert get_service_protocol("kms") == "json"
        assert get_service_protocol("logs") == "json"
        assert get_service_protocol("sqs") == "json"

    def test_rest_json_services(self):
        assert get_service_protocol("lambda") == "rest-json"

    def test_rest_xml_services(self):
        assert get_service_protocol("s3") == "rest-xml"
        assert get_service_protocol("route53") == "rest-xml"

    def test_ec2_protocol(self):
        assert get_service_protocol("ec2") == "ec2"

    def test_unknown_service(self):
        assert get_service_protocol("nonexistent-service-xyz") is None


class TestGetServiceOperations:
    def test_sts_operations(self):
        ops = get_service_operations("sts")
        assert "GetCallerIdentity" in ops
        assert "AssumeRole" in ops

    def test_s3_operations(self):
        ops = get_service_operations("s3")
        assert "CreateBucket" in ops
        assert "PutObject" in ops
        assert "GetObject" in ops

    def test_unknown_service(self):
        ops = get_service_operations("nonexistent-service-xyz")
        assert len(ops) == 0
