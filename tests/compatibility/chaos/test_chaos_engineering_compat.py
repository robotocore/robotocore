"""End-to-end tests for the chaos engineering fault injection feature.

Tests the full lifecycle: add rules, verify fault injection on AWS API calls,
delete rules, and verify recovery. All tests run against the live server.

Note: Chaos error responses are JSON regardless of service protocol. For services
using rest-xml (like S3), boto3 cannot parse __type from JSON and falls back to
using the HTTP status code as the error code. Tests for S3 use raw HTTP to verify
the full error body, while DynamoDB (json protocol) tests use boto3 directly.
"""

import boto3
import pytest
import requests
from botocore.config import Config

from tests.compatibility.conftest import ENDPOINT_URL

CHAOS_URL = f"{ENDPOINT_URL}/_robotocore/chaos/rules"
CHAOS_CLEAR_URL = f"{CHAOS_URL}/clear"

# boto3 retries ThrottlingException and 5xx errors by default.
# Disable retries so chaos-injected errors surface immediately.
NO_RETRY = Config(retries={"max_attempts": 0})
NO_RETRY_S3 = Config(retries={"max_attempts": 0}, s3={"addressing_style": "path"})


def _make_client(service: str):
    """Create a boto3 client with retries disabled."""
    cfg = NO_RETRY_S3 if service == "s3" else NO_RETRY
    return boto3.client(
        service,
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=cfg,
    )


@pytest.fixture(autouse=True)
def _clear_chaos_rules():
    """Clear all chaos rules before and after each test."""
    requests.post(CHAOS_CLEAR_URL)
    yield
    requests.post(CHAOS_CLEAR_URL)


# ---------------------------------------------------------------------------
# 1. ThrottlingException injection
# ---------------------------------------------------------------------------


class TestThrottlingException:
    def test_s3_throttling_via_raw_http(self):
        """Add ThrottlingException rule for S3, verify error via raw HTTP."""
        resp = requests.post(
            CHAOS_URL,
            json={
                "service": "s3",
                "error_code": "ThrottlingException",
                "status_code": 429,
            },
        )
        assert resp.status_code == 201
        rule_id = resp.json()["rule_id"]

        # Raw HTTP request routed to S3
        raw = requests.get(
            f"{ENDPOINT_URL}/",
            headers={
                "Authorization": (
                    "AWS4-HMAC-SHA256 Credential=AKID/20260312/us-east-1/s3/aws4_request, "
                    "SignedHeaders=host, Signature=fake"
                ),
            },
        )
        assert raw.status_code == 429
        assert raw.headers.get("x-robotocore-chaos") == rule_id
        # S3 uses rest-xml protocol, so chaos errors come back as XML
        body = raw.text
        assert "ThrottlingException" in body
        assert "Injected by chaos rule" in body

    def test_dynamodb_throttling_via_boto3(self):
        """Add ThrottlingException for DynamoDB (JSON protocol), verify via boto3."""
        resp = requests.post(
            CHAOS_URL,
            json={
                "service": "dynamodb",
                "error_code": "ThrottlingException",
                "status_code": 429,
            },
        )
        assert resp.status_code == 201

        ddb = _make_client("dynamodb")
        try:
            ddb.list_tables()
            pytest.fail("Expected ClientError from chaos rule")
        except ddb.exceptions.ClientError as e:
            assert e.response["Error"]["Code"] == "ThrottlingException"
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 429


# ---------------------------------------------------------------------------
# 2. ServiceUnavailableException
# ---------------------------------------------------------------------------


class TestServiceUnavailable:
    def test_dynamodb_service_unavailable(self):
        """Add ServiceUnavailableException for DynamoDB, verify error."""
        resp = requests.post(
            CHAOS_URL,
            json={
                "service": "dynamodb",
                "error_code": "ServiceUnavailableException",
                "status_code": 503,
            },
        )
        assert resp.status_code == 201

        ddb = _make_client("dynamodb")
        try:
            ddb.list_tables()
            pytest.fail("Expected ClientError from chaos rule")
        except ddb.exceptions.ClientError as e:
            assert e.response["Error"]["Code"] == "ServiceUnavailableException"
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 503


# ---------------------------------------------------------------------------
# 3. Latency injection
# ---------------------------------------------------------------------------


class TestLatencyInjection:
    def test_latency_rule_is_created_and_matches(self):
        """Add latency rule, verify it appears in rule list and matches requests."""
        resp = requests.post(
            CHAOS_URL,
            json={
                "service": "dynamodb",
                "latency_ms": 500,
            },
        )
        assert resp.status_code == 201
        rule_id = resp.json()["rule_id"]

        # Make a request so the rule matches
        ddb = _make_client("dynamodb")
        ddb.list_tables()

        # Verify match_count incremented
        rules_resp = requests.get(CHAOS_URL)
        rules = rules_resp.json()["rules"]
        matching = [r for r in rules if r["rule_id"] == rule_id]
        assert len(matching) == 1
        assert matching[0]["latency_ms"] == 500
        assert matching[0]["match_count"] >= 1

    def test_latency_with_error_both_apply(self):
        """A rule with both latency and error_code applies both effects."""
        resp = requests.post(
            CHAOS_URL,
            json={
                "service": "dynamodb",
                "latency_ms": 50,
                "error_code": "ThrottlingException",
                "status_code": 429,
            },
        )
        assert resp.status_code == 201

        ddb = _make_client("dynamodb")
        try:
            ddb.list_tables()
            pytest.fail("Expected ClientError")
        except ddb.exceptions.ClientError as e:
            assert e.response["Error"]["Code"] == "ThrottlingException"

        # Verify match count
        rules = requests.get(CHAOS_URL).json()["rules"]
        assert rules[0]["match_count"] >= 1


# ---------------------------------------------------------------------------
# 4. Error rate (probability)
# ---------------------------------------------------------------------------


class TestErrorRate:
    def test_fifty_percent_error_rate(self):
        """50% probability rule: roughly half of 40 requests should fail."""
        resp = requests.post(
            CHAOS_URL,
            json={
                "service": "dynamodb",
                "error_code": "ThrottlingException",
                "status_code": 429,
                "probability": 0.5,
            },
        )
        assert resp.status_code == 201

        ddb = _make_client("dynamodb")
        successes = 0
        failures = 0
        for _ in range(40):
            try:
                ddb.list_tables()
                successes += 1
            except ddb.exceptions.ClientError:
                failures += 1

        # With 40 trials at p=0.5, expect ~20 each.
        # Allow wide margin (5-35) to avoid flaky tests.
        assert failures >= 5, f"Expected at least 5 failures, got {failures}"
        assert successes >= 5, f"Expected at least 5 successes, got {successes}"


# ---------------------------------------------------------------------------
# 5. Service-specific rules
# ---------------------------------------------------------------------------


class TestServiceSpecificRules:
    def test_s3_rule_does_not_affect_dynamodb(self):
        """A rule targeting S3 should not inject faults on DynamoDB requests."""
        resp = requests.post(
            CHAOS_URL,
            json={
                "service": "s3",
                "error_code": "ThrottlingException",
                "status_code": 429,
            },
        )
        assert resp.status_code == 201

        # S3 should fail (verify via raw HTTP since S3 uses rest-xml)
        raw = requests.get(
            f"{ENDPOINT_URL}/",
            headers={
                "Authorization": (
                    "AWS4-HMAC-SHA256 Credential=AKID/20260312/us-east-1/s3/aws4_request, "
                    "SignedHeaders=host, Signature=fake"
                ),
            },
        )
        assert raw.status_code == 429
        # S3 uses rest-xml, so chaos errors come back as XML
        assert "ThrottlingException" in raw.text

        # DynamoDB should succeed (no rule targeting it)
        ddb = _make_client("dynamodb")
        tables = ddb.list_tables()
        assert "TableNames" in tables


# ---------------------------------------------------------------------------
# 6. Operation-specific rules
# ---------------------------------------------------------------------------


class TestOperationSpecificRules:
    def test_rule_for_putobject_does_not_affect_listbuckets(self):
        """A rule matching PutObject should not affect ListBuckets."""
        resp = requests.post(
            CHAOS_URL,
            json={
                "service": "s3",
                "operation": "PutObject",
                "error_code": "ThrottlingException",
                "status_code": 429,
            },
        )
        assert resp.status_code == 201

        s3 = _make_client("s3")
        # ListBuckets should work fine (rule only matches PutObject)
        result = s3.list_buckets()
        assert "Buckets" in result

    def test_rule_for_specific_operation_blocks_it(self):
        """A rule matching CreateTable should block CreateTable requests."""
        resp = requests.post(
            CHAOS_URL,
            json={
                "service": "dynamodb",
                "operation": "CreateTable",
                "error_code": "AccessDeniedException",
                "status_code": 403,
            },
        )
        assert resp.status_code == 201

        ddb = _make_client("dynamodb")

        # ListTables should work (not matched by operation filter)
        result = ddb.list_tables()
        assert "TableNames" in result

        # CreateTable should fail
        try:
            ddb.create_table(
                TableName="chaos-test-table",
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            pytest.fail("Expected ClientError for CreateTable")
        except ddb.exceptions.ClientError as e:
            assert e.response["Error"]["Code"] == "AccessDeniedException"


# ---------------------------------------------------------------------------
# 7. Delete rule
# ---------------------------------------------------------------------------


class TestDeleteRule:
    def test_add_then_delete_rule(self):
        """Add a rule, verify it works, delete it, verify requests succeed."""
        # Add rule
        resp = requests.post(
            CHAOS_URL,
            json={
                "service": "dynamodb",
                "error_code": "ThrottlingException",
                "status_code": 429,
            },
        )
        assert resp.status_code == 201
        rule_id = resp.json()["rule_id"]

        # Verify rule works
        ddb = _make_client("dynamodb")
        try:
            ddb.list_tables()
            pytest.fail("Expected ClientError")
        except ddb.exceptions.ClientError as e:
            assert e.response["Error"]["Code"] == "ThrottlingException"

        # Delete rule
        del_resp = requests.delete(f"{CHAOS_URL}/{rule_id}")
        assert del_resp.status_code == 200
        assert del_resp.json()["status"] == "deleted"

        # Verify DynamoDB works again
        result = ddb.list_tables()
        assert "TableNames" in result

    def test_delete_nonexistent_rule_returns_404(self):
        """Deleting a rule that doesn't exist returns 404."""
        resp = requests.delete(f"{CHAOS_URL}/nonexistent-id")
        assert resp.status_code == 404
        assert "error" in resp.json()


# ---------------------------------------------------------------------------
# 8. List rules
# ---------------------------------------------------------------------------


class TestListRules:
    def test_list_returns_all_rules(self):
        """Add multiple rules, verify list returns all of them."""
        rule_ids = []
        for svc in ["s3", "dynamodb", "sqs"]:
            resp = requests.post(
                CHAOS_URL,
                json={
                    "service": svc,
                    "error_code": "ThrottlingException",
                },
            )
            assert resp.status_code == 201
            rule_ids.append(resp.json()["rule_id"])

        list_resp = requests.get(CHAOS_URL)
        assert list_resp.status_code == 200
        rules = list_resp.json()["rules"]
        assert len(rules) == 3
        returned_ids = {r["rule_id"] for r in rules}
        assert set(rule_ids) == returned_ids

    def test_list_empty_returns_empty_list(self):
        """List with no rules returns empty list."""
        resp = requests.get(CHAOS_URL)
        assert resp.status_code == 200
        assert resp.json()["rules"] == []

    def test_list_includes_rule_fields(self):
        """Verify listed rules contain all expected fields."""
        requests.post(
            CHAOS_URL,
            json={
                "service": "s3",
                "operation": "GetObject",
                "error_code": "SlowDown",
                "status_code": 503,
                "latency_ms": 200,
                "probability": 0.75,
            },
        )

        rules = requests.get(CHAOS_URL).json()["rules"]
        assert len(rules) == 1
        rule = rules[0]
        assert rule["service"] == "s3"
        assert rule["operation"] == "GetObject"
        assert rule["error_code"] == "SlowDown"
        assert rule["status_code"] == 503
        assert rule["latency_ms"] == 200
        assert rule["probability"] == 0.75
        assert "rule_id" in rule
        assert "created_at" in rule
        assert "match_count" in rule


# ---------------------------------------------------------------------------
# 9. Clear all rules
# ---------------------------------------------------------------------------


class TestClearAllRules:
    def test_clear_removes_all_rules(self):
        """Add several rules, clear all, verify clean state."""
        for svc in ["s3", "dynamodb", "sqs", "sns"]:
            requests.post(
                CHAOS_URL,
                json={"service": svc, "error_code": "InternalError"},
            )

        # Verify rules exist
        rules = requests.get(CHAOS_URL).json()["rules"]
        assert len(rules) == 4

        # Clear all
        clear_resp = requests.post(CHAOS_CLEAR_URL)
        assert clear_resp.status_code == 200
        assert clear_resp.json()["count"] == 4

        # Verify empty
        rules = requests.get(CHAOS_URL).json()["rules"]
        assert len(rules) == 0

    def test_clear_on_empty_returns_zero(self):
        """Clearing with no rules returns count 0."""
        resp = requests.post(CHAOS_CLEAR_URL)
        assert resp.status_code == 200
        assert resp.json()["count"] == 0

    def test_services_work_after_clear(self):
        """After clearing rules, all services should work normally."""
        # Add rules for multiple services
        for svc in ["s3", "dynamodb"]:
            requests.post(
                CHAOS_URL,
                json={"service": svc, "error_code": "InternalError", "status_code": 500},
            )

        # Both should fail (verify via raw HTTP to avoid boto3 retry issues)
        raw_s3 = requests.get(
            f"{ENDPOINT_URL}/",
            headers={
                "Authorization": (
                    "AWS4-HMAC-SHA256 Credential=AKID/20260312/us-east-1/s3/aws4_request, "
                    "SignedHeaders=host, Signature=fake"
                ),
            },
        )
        assert raw_s3.status_code == 500

        ddb = _make_client("dynamodb")
        try:
            ddb.list_tables()
            pytest.fail("Expected DynamoDB ClientError")
        except ddb.exceptions.ClientError:
            pass  # resource may already be cleaned up

        # Clear all rules
        requests.post(CHAOS_CLEAR_URL)

        # Both should work now
        s3 = _make_client("s3")
        assert "Buckets" in s3.list_buckets()
        assert "TableNames" in ddb.list_tables()
