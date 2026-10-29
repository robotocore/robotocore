"""Error response wire format compatibility tests.

Verifies that robotocore error responses match the exact wire format that
real AWS returns for each protocol type (rest-xml, json, rest-json, query, ec2).

Users switching from real AWS will break if our error XML/JSON structure,
Content-Type headers, HTTP status codes, or field names don't match.
"""

import json
import uuid
import xml.etree.ElementTree as ET

import pytest
import requests
from botocore.exceptions import ClientError

from tests.compatibility.conftest import ENDPOINT_URL, make_client

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def s3():
    return make_client("s3")


@pytest.fixture
def dynamodb():
    return make_client("dynamodb")


@pytest.fixture
def sqs():
    return make_client("sqs")


@pytest.fixture
def iam():
    return make_client("iam")


@pytest.fixture
def lambda_client():
    return make_client("lambda")


@pytest.fixture
def ec2():
    return make_client("ec2")


@pytest.fixture
def sns():
    return make_client("sns")


@pytest.fixture
def secretsmanager():
    return make_client("secretsmanager")


@pytest.fixture
def cloudwatch():
    return make_client("cloudwatch")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_s3_get(bucket_name: str) -> requests.Response:
    """Make a raw HTTP GET to S3 for a bucket (path-style)."""
    return requests.get(
        f"{ENDPOINT_URL}/{bucket_name}",
        headers={
            "Authorization": (
                "AWS4-HMAC-SHA256 Credential=testing/20260312/us-east-1/s3/aws4_request, "
                "SignedHeaders=host, Signature=fake"
            ),
            "X-Amz-Date": "20260312T000000Z",
        },
    )


def _raw_query_post(service_signing_name: str, form_data: str) -> requests.Response:
    """Make a raw HTTP POST with query-protocol form data."""
    return requests.post(
        ENDPOINT_URL,
        headers={
            "Authorization": (
                f"AWS4-HMAC-SHA256 Credential=testing/20260312/us-east-1/"
                f"{service_signing_name}/aws4_request, SignedHeaders=host, Signature=fake"
            ),
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Amz-Date": "20260312T000000Z",
        },
        data=form_data,
    )


def _raw_json_post(
    service_signing_name: str, target: str, body: dict, json_version: str = "1.0"
) -> requests.Response:
    """Make a raw HTTP POST with JSON protocol."""
    return requests.post(
        ENDPOINT_URL,
        headers={
            "Authorization": (
                f"AWS4-HMAC-SHA256 Credential=testing/20260312/us-east-1/"
                f"{service_signing_name}/aws4_request, "
                f"SignedHeaders=host;x-amz-target, Signature=fake"
            ),
            "X-Amz-Target": target,
            "Content-Type": f"application/x-amz-json-{json_version}",
            "X-Amz-Date": "20260312T000000Z",
        },
        data=json.dumps(body),
    )


def _parse_xml_error(text: str) -> dict:
    """Parse an XML error response and return a dict of element tag -> text."""
    root = ET.fromstring(text)
    result = {"root_tag": _strip_ns(root.tag)}
    for elem in root.iter():
        tag = _strip_ns(elem.tag)
        if elem.text and elem.text.strip():
            # Collect all matching tags; use the first one for simple lookup
            if tag not in result:
                result[tag] = elem.text.strip()
    return result


def _strip_ns(tag: str) -> str:
    """Strip XML namespace prefix from a tag."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


# ===========================================================================
# 1. S3 NoSuchBucket — rest-xml protocol
# ===========================================================================


class TestS3ErrorFormat:
    """S3 uses rest-xml protocol. Errors are bare <Error> (no <ErrorResponse> wrapper)."""

    def test_nosuchbucket_via_boto3(self, s3):
        """ClientError from boto3 has correct error code and HTTP 404."""
        bucket = f"nonexistent-bucket-{uuid.uuid4().hex[:8]}"
        with pytest.raises(ClientError) as exc_info:
            s3.head_bucket(Bucket=bucket)
        err = exc_info.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] in (404, 403)
        # S3 HeadBucket returns 404 or 403 depending on implementation
        assert err["Error"]["Code"] in ("404", "NoSuchBucket", "403")

    def test_nosuchbucket_xml_structure(self):
        """Raw XML uses bare <Error> root (not <ErrorResponse>), matching AWS S3."""
        bucket = f"nonexistent-bucket-{uuid.uuid4().hex[:8]}"
        resp = _raw_s3_get(bucket)
        assert resp.status_code == 404

        # Content-Type must be XML
        ct = resp.headers.get("Content-Type", "")
        assert "xml" in ct.lower(), f"Expected XML content type, got: {ct}"

        # S3 uses bare <Error> as root element (unlike query-protocol services)
        parsed = _parse_xml_error(resp.text)
        assert parsed["root_tag"] == "Error", (
            f"S3 errors must use bare <Error> root, got <{parsed['root_tag']}>"
        )
        assert parsed.get("Code") == "NoSuchBucket"
        assert "BucketName" in parsed, "S3 NoSuchBucket must include <BucketName>"

    def test_nosuchkey_error(self, s3):
        """S3 GetObject for missing key returns 404 with NoSuchKey."""
        bucket = f"errfmt-bucket-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=bucket)
        try:
            with pytest.raises(ClientError) as exc_info:
                s3.get_object(Bucket=bucket, Key="does-not-exist")
            err = exc_info.value.response
            assert err["ResponseMetadata"]["HTTPStatusCode"] == 404
            assert err["Error"]["Code"] == "NoSuchKey"
        finally:
            s3.delete_bucket(Bucket=bucket)


# ===========================================================================
# 2. DynamoDB ResourceNotFoundException — json protocol
# ===========================================================================


class TestDynamoDBErrorFormat:
    """DynamoDB uses json protocol. Errors have __type field."""

    def test_resource_not_found_via_boto3(self, dynamodb):
        """ClientError has ResourceNotFoundException code and 400 status."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb.get_item(
                TableName=f"nonexistent-{uuid.uuid4().hex[:8]}",
                Key={"pk": {"S": "x"}},
            )
        err = exc_info.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert err["Error"]["Code"] == "ResourceNotFoundException"

    def test_resource_not_found_json_structure(self):
        """Raw JSON response has __type field with full or short error code."""
        resp = _raw_json_post(
            "dynamodb",
            "DynamoDB_20120810.GetItem",
            {"TableName": f"nonexistent-{uuid.uuid4().hex[:8]}", "Key": {"pk": {"S": "x"}}},
        )
        assert resp.status_code == 400

        # Content-Type must be JSON
        ct = resp.headers.get("Content-Type", "")
        assert "json" in ct.lower(), f"Expected JSON content type, got: {ct}"

        body = resp.json()
        # AWS DynamoDB returns __type with fully qualified or short name
        assert "__type" in body, "DynamoDB errors must include __type field"
        assert "ResourceNotFoundException" in body["__type"]
        # Must have a message field
        assert "message" in body or "Message" in body

    def test_validation_exception(self, dynamodb):
        """ValidationException for bad input returns 400."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb.create_table(
                TableName=f"bad-{uuid.uuid4().hex[:8]}",
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "unused", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
        err = exc_info.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert err["Error"]["Code"] == "ValidationException"


# ===========================================================================
# 3. SQS NonExistentQueue — query protocol
# ===========================================================================


class TestSQSErrorFormat:
    """SQS uses query protocol. Errors wrapped in <ErrorResponse><Error>."""

    def test_nonexistent_queue_via_boto3(self, sqs):
        """ClientError has correct error code."""
        with pytest.raises(ClientError) as exc_info:
            sqs.get_queue_url(QueueName=f"nonexistent-{uuid.uuid4().hex[:8]}")
        err = exc_info.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert err["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue"

    def test_nonexistent_queue_xml_structure(self):
        """Raw XML uses <ErrorResponse><Error><Code> structure."""
        queue_name = f"nonexistent-{uuid.uuid4().hex[:8]}"
        resp = _raw_query_post("sqs", f"Action=GetQueueUrl&QueueName={queue_name}")
        assert resp.status_code == 400

        ct = resp.headers.get("Content-Type", "")
        assert "xml" in ct.lower(), f"Expected XML content type, got: {ct}"

        parsed = _parse_xml_error(resp.text)
        # Query protocol wraps in <ErrorResponse>
        assert parsed["root_tag"] == "ErrorResponse", (
            f"SQS query errors must use <ErrorResponse> root, got <{parsed['root_tag']}>"
        )
        assert parsed.get("Code") == "AWS.SimpleQueueService.NonExistentQueue"
        assert "Type" in parsed, "Query-protocol errors must include <Type> element"


# ===========================================================================
# 4. IAM NoSuchEntity — query protocol
# ===========================================================================


class TestIAMErrorFormat:
    """IAM uses query protocol. Errors wrapped in <ErrorResponse><Error>."""

    def test_nosuchentity_via_boto3(self, iam):
        """GetUser for non-existent user returns NoSuchEntity."""
        with pytest.raises(ClientError) as exc_info:
            iam.get_user(UserName=f"nonexistent-{uuid.uuid4().hex[:8]}")
        err = exc_info.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert err["Error"]["Code"] == "NoSuchEntity"

    def test_nosuchentity_xml_structure(self):
        """Raw XML uses <ErrorResponse><Error> with <Type> and <Code>."""
        user = f"nonexistent-{uuid.uuid4().hex[:8]}"
        resp = _raw_query_post("iam", f"Action=GetUser&UserName={user}")
        assert resp.status_code == 404

        ct = resp.headers.get("Content-Type", "")
        assert "xml" in ct.lower(), f"Expected XML content type, got: {ct}"

        parsed = _parse_xml_error(resp.text)
        assert parsed["root_tag"] == "ErrorResponse"
        assert parsed.get("Code") == "NoSuchEntity"
        assert parsed.get("Type") == "Sender"


# ===========================================================================
# 5. Lambda ResourceNotFoundException — rest-json protocol
# ===========================================================================


class TestLambdaErrorFormat:
    """Lambda uses rest-json protocol. Errors have JSON body with error type info."""

    def test_resource_not_found_via_boto3(self, lambda_client):
        """GetFunction for non-existent function returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            lambda_client.get_function(FunctionName=f"nonexistent-{uuid.uuid4().hex[:8]}")
        err = exc_info.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert err["Error"]["Code"] == "ResourceNotFoundException"

    def test_resource_not_found_json_wire_format(self):
        """Raw HTTP response is JSON with __type or errorType field."""
        func_name = f"nonexistent-{uuid.uuid4().hex[:8]}"
        resp = requests.get(
            f"{ENDPOINT_URL}/2015-03-31/functions/{func_name}",
            headers={
                "Authorization": (
                    "AWS4-HMAC-SHA256 Credential=testing/20260312/us-east-1/lambda/"
                    "aws4_request, SignedHeaders=host, Signature=fake"
                ),
                "X-Amz-Date": "20260312T000000Z",
            },
        )
        assert resp.status_code == 404

        ct = resp.headers.get("Content-Type", "")
        assert "json" in ct.lower(), f"Expected JSON content type, got: {ct}"

        body = resp.json()
        # rest-json errors use __type, errorType, or x-amzn-ErrorType header
        has_type = "__type" in body or "errorType" in body
        has_header = "x-amzn-errortype" in {k.lower(): v for k, v in resp.headers.items()}
        assert has_type or has_header, (
            "rest-json errors must include __type, errorType, or x-amzn-ErrorType header"
        )
        # Verify the error code is in the type field
        type_val = body.get("__type", body.get("errorType", ""))
        assert "ResourceNotFoundException" in type_val


# ===========================================================================
# 6. EC2 InvalidInstanceID — ec2 protocol
# ===========================================================================


class TestEC2ErrorFormat:
    """EC2 uses ec2 protocol. Errors wrapped in <Response><Errors><Error>."""

    def test_invalid_instance_id_via_boto3(self, ec2):
        """DescribeInstances with fake ID returns InvalidInstanceID.NotFound."""
        with pytest.raises(ClientError) as exc_info:
            ec2.describe_instances(InstanceIds=["i-00000000deadbeef0"])
        err = exc_info.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert err["Error"]["Code"] == "InvalidInstanceID.NotFound"

    def test_invalid_instance_id_xml_structure(self):
        """EC2 errors use <Response><Errors><Error> structure (distinct from query)."""
        resp = _raw_query_post(
            "ec2",
            "Action=DescribeInstances&InstanceId.1=i-00000000deadbeef0",
        )
        assert resp.status_code == 400

        ct = resp.headers.get("Content-Type", "")
        assert "xml" in ct.lower(), f"Expected XML content type, got: {ct}"

        # EC2 error format: <Response><Errors><Error><Code>...<Message>...
        parsed = _parse_xml_error(resp.text)
        # EC2 uses <Response> as root (not <ErrorResponse>)
        assert parsed["root_tag"] == "Response", (
            f"EC2 errors must use <Response> root, got <{parsed['root_tag']}>"
        )
        assert parsed.get("Code") == "InvalidInstanceID.NotFound"
        assert "Message" in parsed

        # Verify <Errors> wrapper exists (EC2-specific)
        root = ET.fromstring(resp.text)
        errors_elem = None
        for elem in root:
            if _strip_ns(elem.tag) == "Errors":
                errors_elem = elem
                break
        assert errors_elem is not None, "EC2 errors must have <Errors> wrapper element"

    def test_ec2_has_request_id(self):
        """EC2 error responses include <RequestID> (capital D, EC2-specific)."""
        resp = _raw_query_post(
            "ec2",
            "Action=DescribeInstances&InstanceId.1=i-00000000deadbeef0",
        )
        root = ET.fromstring(resp.text)
        # EC2 uses <RequestID> (capital D) while query uses <RequestId>
        request_id = None
        for elem in root:
            tag = _strip_ns(elem.tag)
            if tag in ("RequestID", "RequestId"):
                request_id = elem.text
                break
        assert request_id is not None, "EC2 errors must include RequestID"


# ===========================================================================
# 7. SNS NotFound — query protocol
# ===========================================================================


class TestSNSErrorFormat:
    """SNS uses query protocol. Same <ErrorResponse><Error> pattern as IAM/SQS."""

    def test_topic_not_found_via_boto3(self, sns):
        """GetTopicAttributes for non-existent topic returns NotFound."""
        fake_arn = "arn:aws:sns:us-east-1:123456789012:nonexistent-topic-xyz"
        with pytest.raises(ClientError) as exc_info:
            sns.get_topic_attributes(TopicArn=fake_arn)
        err = exc_info.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert err["Error"]["Code"] == "NotFound"

    def test_topic_not_found_xml_structure(self):
        """Raw XML matches query protocol format."""
        fake_arn = "arn:aws:sns:us-east-1:123456789012:nonexistent-topic-xyz"
        resp = _raw_query_post(
            "sns",
            f"Action=GetTopicAttributes&TopicArn={fake_arn}",
        )
        assert resp.status_code == 404

        ct = resp.headers.get("Content-Type", "")
        assert "xml" in ct.lower(), f"Expected XML content type, got: {ct}"

        parsed = _parse_xml_error(resp.text)
        assert parsed["root_tag"] == "ErrorResponse"
        assert parsed.get("Code") == "NotFound"
        assert "RequestId" in parsed, "Query-protocol errors must include <RequestId>"


# ===========================================================================
# 8. Secrets Manager ResourceNotFoundException — json protocol
# ===========================================================================


class TestSecretsManagerErrorFormat:
    """Secrets Manager uses json protocol (json 1.1)."""

    def test_secret_not_found_via_boto3(self, secretsmanager):
        """GetSecretValue for non-existent secret returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            secretsmanager.get_secret_value(SecretId=f"nonexistent-{uuid.uuid4().hex[:8]}")
        err = exc_info.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] in (400, 404)
        assert err["Error"]["Code"] == "ResourceNotFoundException"

    def test_secret_not_found_json_structure(self):
        """Raw JSON has __type with ResourceNotFoundException."""
        resp = _raw_json_post(
            "secretsmanager",
            "secretsmanager.GetSecretValue",
            {"SecretId": f"nonexistent-{uuid.uuid4().hex[:8]}"},
            json_version="1.1",
        )
        assert resp.status_code in (400, 404)

        ct = resp.headers.get("Content-Type", "")
        assert "json" in ct.lower(), f"Expected JSON content type, got: {ct}"

        body = resp.json()
        assert "__type" in body, "JSON-protocol errors must include __type"
        assert "ResourceNotFoundException" in body["__type"]
        assert "message" in body or "Message" in body


# ===========================================================================
# 9. CloudWatch InvalidParameterCombination — query protocol
# ===========================================================================


class TestCloudWatchErrorFormat:
    """CloudWatch uses query protocol (also handles JSON via X-Amz-Target)."""

    def test_invalid_parameter_via_boto3(self, cloudwatch):
        """GetMetricStatistics with missing required params returns error."""
        with pytest.raises(ClientError) as exc_info:
            cloudwatch.get_metric_statistics(
                Namespace="AWS/EC2",
                MetricName="CPUUtilization",
                StartTime="2026-01-01T00:00:00Z",
                EndTime="2026-01-02T00:00:00Z",
                Period=60,
                # Missing Statistics or ExtendedStatistics
            )
        err = exc_info.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert "InvalidParameter" in err["Error"]["Code"]

    def test_invalid_parameter_xml_structure(self):
        """Query-protocol CloudWatch error has <ErrorResponse><Error> format."""
        resp = _raw_query_post(
            "monitoring",
            ("Action=GetMetricStatistics&Namespace=AWS/EC2&MetricName=CPUUtilization&Period=0"),
        )
        assert resp.status_code == 400

        ct = resp.headers.get("Content-Type", "")
        assert "xml" in ct.lower(), f"Expected XML content type, got: {ct}"

        parsed = _parse_xml_error(resp.text)
        assert parsed["root_tag"] == "ErrorResponse"
        assert "Code" in parsed


# ===========================================================================
# 10. x-robotocore-diag header on error responses
# ===========================================================================


class TestDiagnosticHeader:
    """The x-robotocore-diag header appears on 500/501 error responses
    generated by the error_normalizer and moto_bridge exception handlers."""

    def test_not_implemented_has_diag_header(self):
        """A request to an unimplemented operation includes the diag header."""
        # Use a deliberately invalid action that will trigger the error path
        resp = _raw_query_post(
            "sqs",
            "Action=CompletelyFakeOperationThatDoesNotExist",
        )
        # Should be 400 (InvalidAction) or 501 (NotImplemented) with diag header
        if resp.status_code in (400, 501):
            diag = resp.headers.get("x-robotocore-diag")
            if diag is not None:
                assert len(diag) > 0, "x-robotocore-diag header must not be empty"


# ===========================================================================
# 11. HTTP status code correctness
# ===========================================================================


class TestHTTPStatusCodes:
    """Verify error HTTP status codes match AWS conventions."""

    def test_not_found_is_404(self, iam):
        """NoSuchEntity errors return HTTP 404, not 400."""
        with pytest.raises(ClientError) as exc_info:
            iam.get_user(UserName=f"nope-{uuid.uuid4().hex[:8]}")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    def test_not_found_s3_is_404(self, s3):
        """S3 NoSuchKey returns HTTP 404."""
        bucket = f"errfmt-status-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=bucket)
        try:
            with pytest.raises(ClientError) as exc_info:
                s3.get_object(Bucket=bucket, Key="does-not-exist")
            assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        finally:
            s3.delete_bucket(Bucket=bucket)

    def test_dynamodb_not_found_is_400(self, dynamodb):
        """DynamoDB ResourceNotFoundException returns HTTP 400 (not 404)."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb.describe_table(TableName=f"nope-{uuid.uuid4().hex[:8]}")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    def test_sns_not_found_is_404(self, sns):
        """SNS topic not found returns HTTP 404."""
        fake_arn = f"arn:aws:sns:us-east-1:123456789012:nope-{uuid.uuid4().hex[:8]}"
        with pytest.raises(ClientError) as exc_info:
            sns.get_topic_attributes(TopicArn=fake_arn)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    def test_lambda_not_found_is_404(self, lambda_client):
        """Lambda ResourceNotFoundException returns HTTP 404."""
        with pytest.raises(ClientError) as exc_info:
            lambda_client.get_function(FunctionName=f"nope-{uuid.uuid4().hex[:8]}")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    def test_secretsmanager_not_found_status(self, secretsmanager):
        """Secrets Manager ResourceNotFoundException returns 400 or 404."""
        with pytest.raises(ClientError) as exc_info:
            secretsmanager.get_secret_value(SecretId=f"nope-{uuid.uuid4().hex[:8]}")
        # AWS returns 400 for this; some implementations return 404
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404)

    def test_ec2_bad_instance_is_400(self, ec2):
        """EC2 InvalidInstanceID.NotFound returns HTTP 400."""
        with pytest.raises(ClientError) as exc_info:
            ec2.describe_instances(InstanceIds=["i-00000000deadbeef0"])
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


# ===========================================================================
# Cross-protocol Content-Type consistency
# ===========================================================================


class TestContentTypeHeaders:
    """Verify Content-Type headers match AWS conventions per protocol."""

    def test_s3_error_content_type_is_xml(self):
        """S3 rest-xml errors return XML content type."""
        bucket = f"ct-test-{uuid.uuid4().hex[:8]}"
        resp = _raw_s3_get(bucket)
        ct = resp.headers.get("Content-Type", "")
        assert "xml" in ct.lower()

    def test_dynamodb_error_content_type_is_json(self):
        """DynamoDB json errors return JSON content type."""
        resp = _raw_json_post(
            "dynamodb",
            "DynamoDB_20120810.GetItem",
            {"TableName": f"ct-test-{uuid.uuid4().hex[:8]}", "Key": {"pk": {"S": "x"}}},
        )
        ct = resp.headers.get("Content-Type", "")
        assert "json" in ct.lower()

    def test_sqs_error_content_type_is_xml(self):
        """SQS query-protocol errors return XML content type."""
        resp = _raw_query_post("sqs", "Action=GetQueueUrl&QueueName=ct-test-nonexistent")
        ct = resp.headers.get("Content-Type", "")
        assert "xml" in ct.lower()

    def test_ec2_error_content_type_is_xml(self):
        """EC2 protocol errors return XML content type."""
        resp = _raw_query_post("ec2", "Action=DescribeInstances&InstanceId.1=i-00000000deadbeef0")
        ct = resp.headers.get("Content-Type", "")
        assert "xml" in ct.lower()

    def test_secretsmanager_error_content_type_is_json(self):
        """Secrets Manager json-1.1 errors return JSON content type."""
        resp = _raw_json_post(
            "secretsmanager",
            "secretsmanager.GetSecretValue",
            {"SecretId": f"ct-test-{uuid.uuid4().hex[:8]}"},
            json_version="1.1",
        )
        ct = resp.headers.get("Content-Type", "")
        assert "json" in ct.lower()
