"""Tests for IAM policy enforcement: conditions, policy engine, and middleware."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from starlette.responses import Response

from robotocore.gateway.handler_chain import RequestContext
from robotocore.gateway.iam_middleware import (
    _build_access_denied_response,
    build_iam_action,
    build_resource_arn,
    clear_sts_sessions,
    extract_credentials,
    iam_enforcement_handler,
    register_sts_session,
)
from robotocore.services.iam.conditions import (
    _arn_equals,
    _arn_like,
    _arn_not_equals,
    _arn_not_like,
    _bool_op,
    _date_equals,
    _date_greater_than,
    _date_greater_than_equals,
    _date_less_than,
    _date_less_than_equals,
    _date_not_equals,
    _ip_address,
    _not_ip_address,
    _numeric_equals,
    _numeric_greater_than,
    _numeric_greater_than_equals,
    _numeric_less_than,
    _numeric_less_than_equals,
    _numeric_not_equals,
    _string_equals,
    _string_equals_ignore_case,
    _string_like,
    _string_not_equals,
    _string_not_equals_ignore_case,
    _string_not_like,
    evaluate_condition_block,
)
from robotocore.services.iam.policy_engine import (
    ALLOW,
    DENY,
    IMPLICIT_DENY,
    _substitute_variables,
    evaluate_policy,
    evaluate_resource_policy,
    evaluate_with_permission_boundary,
)
from robotocore.services.iam.provider import handle_iam_request

# ===========================================================================
# Policy evaluation basics
# ===========================================================================


class TestPolicyEvaluationBasics:
    def test_allow_policy(self):
        policy = {"Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]}
        assert evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::bucket/key") == ALLOW

    def test_deny_policy(self):
        policy = {"Statement": [{"Effect": "Deny", "Action": "s3:GetObject", "Resource": "*"}]}
        assert evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::bucket/key") == DENY

    def test_implicit_deny(self):
        policy = {"Statement": [{"Effect": "Allow", "Action": "s3:PutObject", "Resource": "*"}]}
        assert evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::bucket/key") == IMPLICIT_DENY

    def test_single_statement_as_dict(self):
        """Statement can be a dict instead of a list."""
        policy = {"Statement": {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}}
        assert evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::bucket/key") == ALLOW

    def test_empty_policies(self):
        assert evaluate_policy([], "s3:GetObject", "arn:aws:s3:::bucket/key") == IMPLICIT_DENY

    def test_no_matching_action(self):
        policy = {"Statement": [{"Effect": "Allow", "Action": "s3:PutObject", "Resource": "*"}]}
        assert evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::bucket/key") == IMPLICIT_DENY

    def test_no_matching_resource(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::other-bucket/*",
                }
            ]
        }
        assert (
            evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::my-bucket/key") == IMPLICIT_DENY
        )

    def test_none_context(self):
        policy = {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
        assert evaluate_policy([policy], "s3:GetObject", "*") == ALLOW


# ===========================================================================
# Explicit deny overrides
# ===========================================================================


class TestExplicitDenyOverrides:
    def test_deny_overrides_allow(self):
        policies = [
            {
                "Statement": [
                    {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
                    {"Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "*"},
                ]
            }
        ]
        assert evaluate_policy(policies, "s3:DeleteObject", "arn:aws:s3:::b/k") == DENY

    def test_deny_in_separate_policy(self):
        allow_policy = {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
        deny_policy = {
            "Statement": [{"Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "*"}]
        }
        assert (
            evaluate_policy([allow_policy, deny_policy], "s3:DeleteObject", "arn:aws:s3:::b/k")
            == DENY
        )

    def test_allow_when_deny_does_not_match(self):
        policies = [
            {
                "Statement": [
                    {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
                    {"Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "*"},
                ]
            }
        ]
        assert evaluate_policy(policies, "s3:GetObject", "arn:aws:s3:::b/k") == ALLOW


# ===========================================================================
# Action matching with wildcards
# ===========================================================================


class TestActionMatching:
    def test_exact_match(self):
        policy = {"Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]}
        assert evaluate_policy([policy], "s3:GetObject", "*") == ALLOW

    def test_wildcard_all(self):
        policy = {"Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]}
        assert evaluate_policy([policy], "sqs:SendMessage", "*") == ALLOW

    def test_service_wildcard(self):
        policy = {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
        assert evaluate_policy([policy], "s3:GetObject", "*") == ALLOW
        assert evaluate_policy([policy], "sqs:SendMessage", "*") == IMPLICIT_DENY

    def test_partial_wildcard(self):
        policy = {"Statement": [{"Effect": "Allow", "Action": "s3:Get*", "Resource": "*"}]}
        assert evaluate_policy([policy], "s3:GetObject", "*") == ALLOW
        assert evaluate_policy([policy], "s3:GetBucketPolicy", "*") == ALLOW
        assert evaluate_policy([policy], "s3:PutObject", "*") == IMPLICIT_DENY

    def test_case_insensitive(self):
        policy = {"Statement": [{"Effect": "Allow", "Action": "S3:getobject", "Resource": "*"}]}
        assert evaluate_policy([policy], "s3:GetObject", "*") == ALLOW

    def test_multiple_actions(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject"],
                    "Resource": "*",
                }
            ]
        }
        assert evaluate_policy([policy], "s3:GetObject", "*") == ALLOW
        assert evaluate_policy([policy], "s3:PutObject", "*") == ALLOW
        assert evaluate_policy([policy], "s3:DeleteObject", "*") == IMPLICIT_DENY


# ===========================================================================
# NotAction / NotResource
# ===========================================================================


class TestNotActionNotResource:
    def test_not_action_allows_other(self):
        policy = {
            "Statement": [{"Effect": "Allow", "NotAction": "s3:DeleteObject", "Resource": "*"}]
        }
        assert evaluate_policy([policy], "s3:GetObject", "*") == ALLOW
        assert evaluate_policy([policy], "s3:DeleteObject", "*") == IMPLICIT_DENY

    def test_not_action_list(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "NotAction": ["s3:DeleteObject", "s3:PutObject"],
                    "Resource": "*",
                }
            ]
        }
        assert evaluate_policy([policy], "s3:GetObject", "*") == ALLOW
        assert evaluate_policy([policy], "s3:DeleteObject", "*") == IMPLICIT_DENY
        assert evaluate_policy([policy], "s3:PutObject", "*") == IMPLICIT_DENY

    def test_not_resource(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:*",
                    "NotResource": "arn:aws:s3:::secret-bucket/*",
                }
            ]
        }
        assert evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::public/k") == ALLOW
        assert (
            evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::secret-bucket/k")
            == IMPLICIT_DENY
        )

    def test_not_resource_list(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:*",
                    "NotResource": [
                        "arn:aws:s3:::secret-bucket/*",
                        "arn:aws:s3:::other-secret/*",
                    ],
                }
            ]
        }
        assert evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::public/k") == ALLOW
        assert (
            evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::secret-bucket/k")
            == IMPLICIT_DENY
        )


# ===========================================================================
# Resource ARN matching
# ===========================================================================


class TestResourceArnMatching:
    def test_exact_arn(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-bucket/my-key",
                }
            ]
        }
        assert evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::my-bucket/my-key") == ALLOW
        assert (
            evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::my-bucket/other")
            == IMPLICIT_DENY
        )

    def test_wildcard_arn(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-bucket/*",
                }
            ]
        }
        assert evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::my-bucket/any-key") == ALLOW

    def test_star_resource(self):
        policy = {"Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]}
        assert evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::any-bucket/any-key") == ALLOW

    def test_multiple_resources(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": [
                        "arn:aws:s3:::bucket-a/*",
                        "arn:aws:s3:::bucket-b/*",
                    ],
                }
            ]
        }
        assert evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::bucket-a/key") == ALLOW
        assert evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::bucket-b/key") == ALLOW
        assert (
            evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::bucket-c/key") == IMPLICIT_DENY
        )


# ===========================================================================
# Policy variable substitution
# ===========================================================================


class TestPolicyVariableSubstitution:
    def test_aws_username(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:*",
                    "Resource": "arn:aws:s3:::bucket/${aws:username}/*",
                }
            ]
        }
        ctx = {"aws:username": "alice"}
        assert (
            evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::bucket/alice/file", ctx)
            == ALLOW
        )
        assert (
            evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::bucket/bob/file", ctx)
            == IMPLICIT_DENY
        )

    def test_s3_prefix(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:*",
                    "Resource": "arn:aws:s3:::bucket/${s3:prefix}*",
                }
            ]
        }
        ctx = {"s3:prefix": "home/"}
        assert (
            evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::bucket/home/file.txt", ctx)
            == ALLOW
        )

    def test_unknown_variable_not_substituted(self):
        result = _substitute_variables("${unknown:var}", {})
        assert result == "${unknown:var}"

    def test_multiple_variables(self):
        result = _substitute_variables(
            "arn:aws:s3:::${aws:username}/${s3:prefix}*",
            {"aws:username": "alice", "s3:prefix": "data/"},
        )
        assert result == "arn:aws:s3:::alice/data/*"


# ===========================================================================
# String condition operators
# ===========================================================================


class TestStringConditions:
    def test_string_equals(self):
        assert _string_equals("foo", "foo") is True
        assert _string_equals("foo", "bar") is False

    def test_string_not_equals(self):
        assert _string_not_equals("foo", "bar") is True
        assert _string_not_equals("foo", "foo") is False

    def test_string_equals_ignore_case(self):
        assert _string_equals_ignore_case("Foo", "foo") is True
        assert _string_equals_ignore_case("FOO", "fOo") is True
        assert _string_equals_ignore_case("foo", "bar") is False

    def test_string_not_equals_ignore_case(self):
        assert _string_not_equals_ignore_case("foo", "bar") is True
        assert _string_not_equals_ignore_case("Foo", "foo") is False

    def test_string_like(self):
        assert _string_like("test-value", "test-*") is True
        assert _string_like("test-value", "test-?alue") is True
        assert _string_like("other", "test-*") is False

    def test_string_not_like(self):
        assert _string_not_like("other", "test-*") is True
        assert _string_not_like("test-value", "test-*") is False


# ===========================================================================
# Numeric condition operators
# ===========================================================================


class TestNumericConditions:
    def test_numeric_equals(self):
        assert _numeric_equals(42, 42) is True
        assert _numeric_equals("42", "42.0") is True
        assert _numeric_equals(42, 43) is False

    def test_numeric_not_equals(self):
        assert _numeric_not_equals(42, 43) is True
        assert _numeric_not_equals(42, 42) is False

    def test_numeric_less_than(self):
        assert _numeric_less_than(5, 10) is True
        assert _numeric_less_than(10, 5) is False
        assert _numeric_less_than(5, 5) is False

    def test_numeric_less_than_equals(self):
        assert _numeric_less_than_equals(5, 10) is True
        assert _numeric_less_than_equals(5, 5) is True
        assert _numeric_less_than_equals(10, 5) is False

    def test_numeric_greater_than(self):
        assert _numeric_greater_than(10, 5) is True
        assert _numeric_greater_than(5, 10) is False

    def test_numeric_greater_than_equals(self):
        assert _numeric_greater_than_equals(10, 5) is True
        assert _numeric_greater_than_equals(5, 5) is True
        assert _numeric_greater_than_equals(4, 5) is False


# ===========================================================================
# Date condition operators
# ===========================================================================


class TestDateConditions:
    def test_date_equals(self):
        assert _date_equals("2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z") is True
        assert _date_equals("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z") is False

    def test_date_not_equals(self):
        assert _date_not_equals("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z") is True
        assert _date_not_equals("2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z") is False

    def test_date_less_than(self):
        assert _date_less_than("2024-01-01T00:00:00Z", "2024-06-01T00:00:00Z") is True
        assert _date_less_than("2024-06-01T00:00:00Z", "2024-01-01T00:00:00Z") is False

    def test_date_less_than_equals(self):
        assert _date_less_than_equals("2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z") is True
        assert _date_less_than_equals("2024-06-01T00:00:00Z", "2024-01-01T00:00:00Z") is False

    def test_date_greater_than(self):
        assert _date_greater_than("2024-06-01T00:00:00Z", "2024-01-01T00:00:00Z") is True
        assert _date_greater_than("2024-01-01T00:00:00Z", "2024-06-01T00:00:00Z") is False

    def test_date_greater_than_equals(self):
        assert _date_greater_than_equals("2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z") is True

    def test_date_with_datetime_objects(self):
        dt1 = datetime(2024, 1, 1, tzinfo=UTC)
        dt2 = datetime(2024, 6, 1, tzinfo=UTC)
        assert _date_less_than(dt1, dt2) is True

    def test_date_without_timezone(self):
        """Dates without timezone should be treated as UTC."""
        assert _date_equals("2024-01-01T00:00:00", "2024-01-01T00:00:00Z") is True


# ===========================================================================
# Bool condition operator
# ===========================================================================


class TestBoolCondition:
    def test_bool_true(self):
        assert _bool_op("true", "true") is True
        assert _bool_op(True, "true") is True
        assert _bool_op("true", True) is True

    def test_bool_false(self):
        assert _bool_op("false", "false") is True
        assert _bool_op(False, "false") is True

    def test_bool_mismatch(self):
        assert _bool_op("true", "false") is False
        assert _bool_op("false", "true") is False


# ===========================================================================
# IP condition operators
# ===========================================================================


class TestIpConditions:
    def test_ip_in_cidr(self):
        assert _ip_address("192.168.1.100", "192.168.1.0/24") is True
        assert _ip_address("10.0.0.1", "192.168.1.0/24") is False

    def test_ip_exact(self):
        assert _ip_address("192.168.1.1", "192.168.1.1/32") is True
        assert _ip_address("192.168.1.2", "192.168.1.1/32") is False

    def test_not_ip_address(self):
        assert _not_ip_address("10.0.0.1", "192.168.1.0/24") is True
        assert _not_ip_address("192.168.1.100", "192.168.1.0/24") is False

    def test_ip_wide_cidr(self):
        assert _ip_address("10.255.255.255", "10.0.0.0/8") is True


# ===========================================================================
# ARN condition operators
# ===========================================================================


class TestArnConditions:
    def test_arn_equals(self):
        arn = "arn:aws:s3:::my-bucket"
        assert _arn_equals(arn, "arn:aws:s3:::my-bucket") is True
        assert _arn_equals(arn, "arn:aws:s3:::other-bucket") is False

    def test_arn_not_equals(self):
        assert _arn_not_equals("arn:aws:s3:::a", "arn:aws:s3:::b") is True
        assert _arn_not_equals("arn:aws:s3:::a", "arn:aws:s3:::a") is False

    def test_arn_like(self):
        assert _arn_like("arn:aws:s3:::my-bucket", "arn:aws:s3:::my-*") is True
        assert _arn_like("arn:aws:s3:::my-bucket", "arn:aws:s3:::other-*") is False

    def test_arn_not_like(self):
        assert _arn_not_like("arn:aws:s3:::my-bucket", "arn:aws:s3:::other-*") is True
        assert _arn_not_like("arn:aws:s3:::my-bucket", "arn:aws:s3:::my-*") is False

    def test_arn_like_per_section_wildcard(self):
        """Wildcards should match within each ARN section independently."""
        assert (
            _arn_like(
                "arn:aws:iam::123456789012:user/alice",
                "arn:aws:iam::*:user/*",
            )
            is True
        )

    def test_arn_short_arn_returns_false(self):
        assert _arn_equals("not-an-arn", "also-not") is False


# ===========================================================================
# Null condition operator
# ===========================================================================


class TestNullCondition:
    def test_null_key_absent(self):
        ctx = {}
        result = evaluate_condition_block({"Null": {"aws:TokenIssueTime": "true"}}, ctx)
        assert result is True

    def test_null_key_present(self):
        ctx = {"aws:TokenIssueTime": "2024-01-01"}
        result = evaluate_condition_block({"Null": {"aws:TokenIssueTime": "true"}}, ctx)
        assert result is False

    def test_null_false_key_present(self):
        ctx = {"aws:TokenIssueTime": "2024-01-01"}
        result = evaluate_condition_block({"Null": {"aws:TokenIssueTime": "false"}}, ctx)
        assert result is True

    def test_null_false_key_absent(self):
        ctx = {}
        result = evaluate_condition_block({"Null": {"aws:TokenIssueTime": "false"}}, ctx)
        assert result is False

    def test_null_with_none_value(self):
        ctx: dict[str, Any] = {"aws:TokenIssueTime": None}
        result = evaluate_condition_block({"Null": {"aws:TokenIssueTime": "true"}}, ctx)
        assert result is True


# ===========================================================================
# Set operators (ForAllValues, ForAnyValue)
# ===========================================================================


class TestSetOperators:
    def test_for_all_values_all_match(self):
        ctx = {"aws:TagKeys": ["env", "team"]}
        result = evaluate_condition_block(
            {"ForAllValues:StringEquals": {"aws:TagKeys": ["env", "team", "project"]}},
            ctx,
        )
        assert result is True

    def test_for_all_values_not_all_match(self):
        ctx = {"aws:TagKeys": ["env", "secret"]}
        result = evaluate_condition_block(
            {"ForAllValues:StringEquals": {"aws:TagKeys": ["env", "team"]}},
            ctx,
        )
        assert result is False

    def test_for_any_value_one_match(self):
        ctx = {"aws:TagKeys": ["env", "other"]}
        result = evaluate_condition_block(
            {"ForAnyValue:StringEquals": {"aws:TagKeys": ["env", "team"]}},
            ctx,
        )
        assert result is True

    def test_for_any_value_no_match(self):
        ctx = {"aws:TagKeys": ["foo", "bar"]}
        result = evaluate_condition_block(
            {"ForAnyValue:StringEquals": {"aws:TagKeys": ["env", "team"]}},
            ctx,
        )
        assert result is False

    def test_for_all_values_empty_context(self):
        """ForAllValues with no context values is vacuously true."""
        ctx: dict[str, Any] = {}
        result = evaluate_condition_block(
            {"ForAllValues:StringEquals": {"aws:TagKeys": ["env"]}},
            ctx,
        )
        assert result is True

    def test_for_any_value_empty_context(self):
        """ForAnyValue with no context values is false."""
        ctx: dict[str, Any] = {}
        result = evaluate_condition_block(
            {"ForAnyValue:StringEquals": {"aws:TagKeys": ["env"]}},
            ctx,
        )
        assert result is False

    def test_for_all_values_single_context_value(self):
        """Single (non-list) context value should work."""
        ctx = {"aws:PrincipalTag/dept": "engineering"}
        result = evaluate_condition_block(
            {"ForAllValues:StringEquals": {"aws:PrincipalTag/dept": ["engineering", "science"]}},
            ctx,
        )
        assert result is True


# ===========================================================================
# IfExists suffix
# ===========================================================================


class TestIfExists:
    def test_if_exists_key_present(self):
        ctx = {"aws:SourceVpc": "vpc-123"}
        result = evaluate_condition_block(
            {"StringEqualsIfExists": {"aws:SourceVpc": "vpc-123"}}, ctx
        )
        assert result is True

    def test_if_exists_key_present_no_match(self):
        ctx = {"aws:SourceVpc": "vpc-999"}
        result = evaluate_condition_block(
            {"StringEqualsIfExists": {"aws:SourceVpc": "vpc-123"}}, ctx
        )
        assert result is False

    def test_if_exists_key_absent(self):
        """If key is absent and IfExists is used, condition is satisfied."""
        ctx: dict[str, Any] = {}
        result = evaluate_condition_block(
            {"StringEqualsIfExists": {"aws:SourceVpc": "vpc-123"}}, ctx
        )
        assert result is True

    def test_without_if_exists_key_absent(self):
        """Without IfExists, missing key means condition fails."""
        ctx: dict[str, Any] = {}
        result = evaluate_condition_block({"StringEquals": {"aws:SourceVpc": "vpc-123"}}, ctx)
        assert result is False


# ===========================================================================
# Multiple conditions (AND logic)
# ===========================================================================


class TestMultipleConditions:
    def test_all_conditions_must_match(self):
        ctx = {"aws:SourceIp": "192.168.1.100", "aws:SecureTransport": "true"}
        result = evaluate_condition_block(
            {
                "IpAddress": {"aws:SourceIp": "192.168.1.0/24"},
                "Bool": {"aws:SecureTransport": "true"},
            },
            ctx,
        )
        assert result is True

    def test_one_condition_fails(self):
        ctx = {"aws:SourceIp": "10.0.0.1", "aws:SecureTransport": "true"}
        result = evaluate_condition_block(
            {
                "IpAddress": {"aws:SourceIp": "192.168.1.0/24"},
                "Bool": {"aws:SecureTransport": "true"},
            },
            ctx,
        )
        assert result is False

    def test_multiple_keys_in_one_operator(self):
        """Multiple keys under the same operator must all match (AND)."""
        ctx = {"aws:PrincipalTag/dept": "eng", "aws:PrincipalTag/team": "infra"}
        result = evaluate_condition_block(
            {
                "StringEquals": {
                    "aws:PrincipalTag/dept": "eng",
                    "aws:PrincipalTag/team": "infra",
                }
            },
            ctx,
        )
        assert result is True

    def test_multiple_values_for_one_key(self):
        """Multiple values for one key means OR (any value matches)."""
        ctx = {"aws:PrincipalTag/dept": "eng"}
        result = evaluate_condition_block(
            {"StringEquals": {"aws:PrincipalTag/dept": ["eng", "science"]}},
            ctx,
        )
        assert result is True


# ===========================================================================
# Conditions in policy statements
# ===========================================================================


class TestConditionsInStatements:
    def test_allow_with_matching_condition(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": "*",
                    "Condition": {"IpAddress": {"aws:SourceIp": "10.0.0.0/8"}},
                }
            ]
        }
        ctx = {"aws:SourceIp": "10.1.2.3"}
        assert evaluate_policy([policy], "s3:GetObject", "*", ctx) == ALLOW

    def test_allow_with_non_matching_condition(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": "*",
                    "Condition": {"IpAddress": {"aws:SourceIp": "10.0.0.0/8"}},
                }
            ]
        }
        ctx = {"aws:SourceIp": "192.168.1.1"}
        assert evaluate_policy([policy], "s3:GetObject", "*", ctx) == IMPLICIT_DENY

    def test_deny_with_matching_condition(self):
        policy = {
            "Statement": [
                {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
                {
                    "Effect": "Deny",
                    "Action": "s3:*",
                    "Resource": "*",
                    "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                },
            ]
        }
        ctx = {"aws:SecureTransport": "false"}
        assert evaluate_policy([policy], "s3:GetObject", "*", ctx) == DENY

    def test_deny_with_non_matching_condition(self):
        policy = {
            "Statement": [
                {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
                {
                    "Effect": "Deny",
                    "Action": "s3:*",
                    "Resource": "*",
                    "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                },
            ]
        }
        ctx = {"aws:SecureTransport": "true"}
        assert evaluate_policy([policy], "s3:GetObject", "*", ctx) == ALLOW


# ===========================================================================
# Permission boundaries
# ===========================================================================


class TestPermissionBoundaries:
    def test_boundary_allows(self):
        identity = {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
        boundary = {"Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]}
        assert evaluate_with_permission_boundary([identity], boundary, "s3:GetObject", "*") == ALLOW

    def test_boundary_denies(self):
        identity = {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
        boundary = {"Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]}
        assert (
            evaluate_with_permission_boundary([identity], boundary, "s3:PutObject", "*")
            == IMPLICIT_DENY
        )

    def test_no_boundary(self):
        identity = {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
        assert evaluate_with_permission_boundary([identity], None, "s3:PutObject", "*") == ALLOW

    def test_explicit_deny_overrides_boundary(self):
        identity = {"Statement": [{"Effect": "Deny", "Action": "s3:*", "Resource": "*"}]}
        boundary = {"Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]}
        assert evaluate_with_permission_boundary([identity], boundary, "s3:GetObject", "*") == DENY

    def test_boundary_with_context(self):
        identity = {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
        boundary = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:*",
                    "Resource": "*",
                    "Condition": {"IpAddress": {"aws:SourceIp": "10.0.0.0/8"}},
                }
            ]
        }
        ctx = {"aws:SourceIp": "10.1.2.3"}
        assert (
            evaluate_with_permission_boundary([identity], boundary, "s3:GetObject", "*", ctx)
            == ALLOW
        )
        ctx2 = {"aws:SourceIp": "192.168.1.1"}
        assert (
            evaluate_with_permission_boundary([identity], boundary, "s3:GetObject", "*", ctx2)
            == IMPLICIT_DENY
        )


# ===========================================================================
# Resource policy evaluation
# ===========================================================================


class TestResourcePolicyEvaluation:
    def test_principal_star(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "sqs:SendMessage",
                    "Resource": "*",
                }
            ]
        }
        assert (
            evaluate_resource_policy(
                policy, "arn:aws:iam::123456789012:user/alice", "sqs:SendMessage", "*"
            )
            == ALLOW
        )

    def test_principal_specific(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                    "Action": "sqs:SendMessage",
                    "Resource": "*",
                }
            ]
        }
        assert (
            evaluate_resource_policy(
                policy, "arn:aws:iam::123456789012:root", "sqs:SendMessage", "*"
            )
            == ALLOW
        )
        assert (
            evaluate_resource_policy(
                policy, "arn:aws:iam::999999999999:root", "sqs:SendMessage", "*"
            )
            == IMPLICIT_DENY
        )

    def test_principal_list(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": [
                            "arn:aws:iam::111111111111:root",
                            "arn:aws:iam::222222222222:root",
                        ]
                    },
                    "Action": "s3:GetObject",
                    "Resource": "*",
                }
            ]
        }
        assert (
            evaluate_resource_policy(policy, "arn:aws:iam::111111111111:root", "s3:GetObject", "*")
            == ALLOW
        )
        assert (
            evaluate_resource_policy(policy, "arn:aws:iam::333333333333:root", "s3:GetObject", "*")
            == IMPLICIT_DENY
        )

    def test_resource_policy_deny(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:*",
                    "Resource": "*",
                }
            ]
        }
        assert (
            evaluate_resource_policy(policy, "arn:aws:iam::123456789012:root", "s3:GetObject", "*")
            == DENY
        )

    def test_resource_policy_with_condition(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "sqs:SendMessage",
                    "Resource": "*",
                    "Condition": {
                        "ArnEquals": {
                            "aws:SourceArn": "arn:aws:sns:us-east-1:123456789012:my-topic"
                        }
                    },
                }
            ]
        }
        ctx = {"aws:SourceArn": "arn:aws:sns:us-east-1:123456789012:my-topic"}
        assert evaluate_resource_policy(policy, "*", "sqs:SendMessage", "*", ctx) == ALLOW
        ctx2 = {"aws:SourceArn": "arn:aws:sns:us-east-1:123456789012:other-topic"}
        assert evaluate_resource_policy(policy, "*", "sqs:SendMessage", "*", ctx2) == IMPLICIT_DENY


# ===========================================================================
# Cross-account access
# ===========================================================================


class TestCrossAccountAccess:
    def test_cross_account_principal(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "arn:aws:iam::999999999999:root"},
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::shared-bucket/*",
                }
            ]
        }
        assert (
            evaluate_resource_policy(
                policy,
                "arn:aws:iam::999999999999:root",
                "s3:GetObject",
                "arn:aws:s3:::shared-bucket/file",
            )
            == ALLOW
        )
        assert (
            evaluate_resource_policy(
                policy,
                "arn:aws:iam::111111111111:root",
                "s3:GetObject",
                "arn:aws:s3:::shared-bucket/file",
            )
            == IMPLICIT_DENY
        )

    def test_cross_account_wildcard(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "arn:aws:iam::*:root"},
                    "Action": "s3:GetObject",
                    "Resource": "*",
                }
            ]
        }
        assert (
            evaluate_resource_policy(
                policy,
                "arn:aws:iam::999999999999:root",
                "s3:GetObject",
                "*",
            )
            == ALLOW
        )


# ===========================================================================
# Credential extraction
# ===========================================================================


class TestCredentialExtraction:
    def test_sigv4_header(self):
        request = MagicMock()
        request.headers = {
            "authorization": (
                "AWS4-HMAC-SHA256 "
                "Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/s3/aws4_request, "
                "SignedHeaders=host;x-amz-date, "
                "Signature=abc123"
            )
        }
        request.query_params = {}
        creds = extract_credentials(request)
        assert creds is not None
        assert creds["access_key_id"] == "AKIAIOSFODNN7EXAMPLE"
        assert creds["region"] == "us-east-1"
        assert creds["service"] == "s3"

    def test_presigned_url(self):
        request = MagicMock()
        request.headers = {}
        request.query_params = {
            "X-Amz-Credential": "AKIAIOSFODNN7EXAMPLE/20240101/eu-west-1/sqs/aws4_request"
        }
        creds = extract_credentials(request)
        assert creds is not None
        assert creds["access_key_id"] == "AKIAIOSFODNN7EXAMPLE"
        assert creds["region"] == "eu-west-1"
        assert creds["service"] == "sqs"

    def test_no_credentials(self):
        request = MagicMock()
        request.headers = {}
        request.query_params = {}
        assert extract_credentials(request) is None

    def test_malformed_auth_header(self):
        request = MagicMock()
        request.headers = {"authorization": "Basic dXNlcjpwYXNz"}
        request.query_params = {}
        assert extract_credentials(request) is None


# ===========================================================================
# IAM action building
# ===========================================================================


class TestIamActionBuilding:
    def test_sqs_action(self):
        assert build_iam_action("sqs", "SendMessage") == "sqs:SendMessage"

    def test_lambda_action(self):
        assert build_iam_action("lambda", "Invoke") == "lambda:Invoke"

    def test_dynamodb_action(self):
        assert build_iam_action("dynamodb", "GetItem") == "dynamodb:GetItem"

    def test_monitoring_maps_to_cloudwatch(self):
        assert build_iam_action("monitoring", "PutMetricData") == "cloudwatch:PutMetricData"

    def test_stepfunctions_maps_to_states(self):
        assert build_iam_action("stepfunctions", "StartExecution") == "states:StartExecution"

    def test_no_operation(self):
        assert build_iam_action("s3", None) == "s3:*"

    def test_unknown_service(self):
        assert build_iam_action("newservice", "DoThing") == "newservice:DoThing"


# ===========================================================================
# Resource ARN building
# ===========================================================================


class TestResourceArnBuilding:
    def test_s3_bucket_and_key(self):
        request = MagicMock()
        request.url.path = "/my-bucket/my-key"
        request.query_params = {}
        arn = build_resource_arn("s3", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:s3:::my-bucket/my-key"

    def test_s3_bucket_only(self):
        request = MagicMock()
        request.url.path = "/my-bucket"
        request.query_params = {}
        arn = build_resource_arn("s3", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:s3:::my-bucket"

    def test_sqs_from_queue_url(self):
        request = MagicMock()
        request.url.path = "/"
        request.query_params = {"QueueUrl": "http://localhost:4566/123456789012/my-queue"}
        arn = build_resource_arn("sqs", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:sqs:us-east-1:123456789012:my-queue"

    def test_sqs_from_path(self):
        request = MagicMock()
        request.url.path = "/123456789012/my-queue"
        request.query_params = {}
        arn = build_resource_arn("sqs", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:sqs:us-east-1:123456789012:my-queue"

    def test_lambda_function(self):
        request = MagicMock()
        request.url.path = "/2015-03-31/functions/my-func/invocations"
        request.query_params = {}
        arn = build_resource_arn("lambda", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:lambda:us-east-1:123456789012:function:my-func"

    def test_sns_from_topic_arn(self):
        request = MagicMock()
        request.url.path = "/"
        request.query_params = {"TopicArn": "arn:aws:sns:us-east-1:123456789012:my-topic"}
        arn = build_resource_arn("sns", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:sns:us-east-1:123456789012:my-topic"

    def test_dynamodb_fallback(self):
        request = MagicMock()
        request.url.path = "/"
        request.query_params = {}
        arn = build_resource_arn("dynamodb", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:dynamodb:us-east-1:123456789012:table/*"

    def test_generic_fallback(self):
        request = MagicMock()
        request.url.path = "/"
        request.query_params = {}
        arn = build_resource_arn("kms", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:kms:us-east-1:123456789012:*"


# ===========================================================================
# Middleware integration
# ===========================================================================


class TestMiddlewareIntegration:
    def _make_context(
        self,
        service: str = "s3",
        operation: str | None = "GetObject",
        auth: str = "",
    ) -> RequestContext:
        request = MagicMock()
        request.headers = {"authorization": auth} if auth else {}
        request.query_params = {}
        request.url.path = "/"
        request.client = MagicMock()
        request.client.host = "127.0.0.1"
        ctx = RequestContext(request=request, service_name=service)
        ctx.operation = operation
        ctx.region = "us-east-1"
        ctx.account_id = "123456789012"
        return ctx

    def test_disabled_by_default(self):
        ctx = self._make_context()
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("ENFORCE_IAM", None)
            iam_enforcement_handler(ctx)
        assert ctx.response is None

    def test_skip_iam_service(self):
        ctx = self._make_context(service="iam")
        with patch.dict(os.environ, {"ENFORCE_IAM": "1"}):
            iam_enforcement_handler(ctx)
        assert ctx.response is None

    def test_skip_sts_service(self):
        ctx = self._make_context(service="sts")
        with patch.dict(os.environ, {"ENFORCE_IAM": "1"}):
            iam_enforcement_handler(ctx)
        assert ctx.response is None

    def test_no_credentials_allows(self):
        ctx = self._make_context()
        with patch.dict(os.environ, {"ENFORCE_IAM": "1"}):
            iam_enforcement_handler(ctx)
        assert ctx.response is None

    def test_allow_with_policy(self):
        auth = (
            "AWS4-HMAC-SHA256 "
            "Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, "
            "SignedHeaders=host, Signature=abc"
        )
        ctx = self._make_context(auth=auth)
        allow_policy = {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
        with (
            patch.dict(os.environ, {"ENFORCE_IAM": "1"}),
            patch(
                "robotocore.gateway.iam_middleware._gather_policies",
                return_value=[allow_policy],
            ),
        ):
            iam_enforcement_handler(ctx)
        assert ctx.response is None

    def test_deny_with_policy(self):
        auth = (
            "AWS4-HMAC-SHA256 "
            "Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, "
            "SignedHeaders=host, Signature=abc"
        )
        ctx = self._make_context(auth=auth)
        deny_policy = {"Statement": [{"Effect": "Deny", "Action": "s3:*", "Resource": "*"}]}
        with (
            patch.dict(os.environ, {"ENFORCE_IAM": "1"}),
            patch(
                "robotocore.gateway.iam_middleware._gather_policies",
                return_value=[deny_policy],
            ),
        ):
            iam_enforcement_handler(ctx)
        assert ctx.response is not None
        assert ctx.response.status_code == 403

    def test_implicit_deny_no_policies(self):
        auth = (
            "AWS4-HMAC-SHA256 "
            "Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, "
            "SignedHeaders=host, Signature=abc"
        )
        ctx = self._make_context(auth=auth)
        with (
            patch.dict(os.environ, {"ENFORCE_IAM": "1"}),
            patch(
                "robotocore.gateway.iam_middleware._gather_policies",
                return_value=[],
            ),
        ):
            iam_enforcement_handler(ctx)
        assert ctx.response is not None
        assert ctx.response.status_code == 403


# ===========================================================================
# Access denied response formats
# ===========================================================================


class TestAccessDeniedResponses:
    def test_json_format(self):
        resp = _build_access_denied_response("s3:GetObject", "json")
        assert resp.status_code == 403
        import json

        body = json.loads(resp.body)
        assert body["__type"] == "AccessDeniedException"
        assert "s3:GetObject" in body["Message"]

    def test_rest_json_format(self):
        resp = _build_access_denied_response("lambda:Invoke", "rest-json")
        assert resp.status_code == 403
        import json

        body = json.loads(resp.body)
        assert body["__type"] == "AccessDeniedException"

    def test_xml_format(self):
        resp = _build_access_denied_response("sqs:SendMessage", None)
        assert resp.status_code == 403
        body = resp.body.decode() if isinstance(resp.body, bytes) else resp.body
        assert "<Code>AccessDenied</Code>" in body
        assert "sqs:SendMessage" in body

    def test_xml_format_query_protocol(self):
        resp = _build_access_denied_response("sqs:SendMessage", "query")
        assert resp.status_code == 403
        body = resp.body.decode() if isinstance(resp.body, bytes) else resp.body
        assert "<ErrorResponse>" in body


# ===========================================================================
# Complex realistic scenarios
# ===========================================================================


class TestComplexScenarios:
    def test_admin_user(self):
        """Admin user with full access."""
        policy = {"Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]}
        assert evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::any/key") == ALLOW
        assert (
            evaluate_policy([policy], "ec2:RunInstances", "arn:aws:ec2:us-east-1:123:instance/*")
            == ALLOW
        )
        assert evaluate_policy([policy], "iam:CreateUser", "*") == ALLOW

    def test_readonly_user(self):
        """Read-only user across all services."""
        policy = {
            "Statement": [
                {"Effect": "Allow", "Action": ["s3:Get*", "s3:List*"], "Resource": "*"},
                {
                    "Effect": "Allow",
                    "Action": ["dynamodb:GetItem", "dynamodb:Query", "dynamodb:Scan"],
                    "Resource": "*",
                },
            ]
        }
        assert evaluate_policy([policy], "s3:GetObject", "*") == ALLOW
        assert evaluate_policy([policy], "s3:ListBuckets", "*") == ALLOW
        assert evaluate_policy([policy], "s3:PutObject", "*") == IMPLICIT_DENY
        assert evaluate_policy([policy], "dynamodb:GetItem", "*") == ALLOW
        assert evaluate_policy([policy], "dynamodb:PutItem", "*") == IMPLICIT_DENY

    def test_scoped_user(self):
        """User scoped to specific bucket with IP restriction."""
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:*",
                    "Resource": [
                        "arn:aws:s3:::user-bucket",
                        "arn:aws:s3:::user-bucket/*",
                    ],
                    "Condition": {"IpAddress": {"aws:SourceIp": "10.0.0.0/8"}},
                }
            ]
        }
        ctx = {"aws:SourceIp": "10.1.2.3"}
        assert (
            evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::user-bucket/file", ctx) == ALLOW
        )
        assert (
            evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::other-bucket/file", ctx)
            == IMPLICIT_DENY
        )
        bad_ctx = {"aws:SourceIp": "192.168.1.1"}
        assert (
            evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::user-bucket/file", bad_ctx)
            == IMPLICIT_DENY
        )

    def test_multi_policy_evaluation(self):
        """Multiple policies from different sources (user + group)."""
        user_policy = {
            "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]
        }
        group_policy = {
            "Statement": [
                {"Effect": "Allow", "Action": "sqs:*", "Resource": "*"},
                {
                    "Effect": "Deny",
                    "Action": "sqs:DeleteQueue",
                    "Resource": "*",
                },
            ]
        }
        policies = [user_policy, group_policy]
        assert evaluate_policy(policies, "s3:GetObject", "*") == ALLOW
        assert evaluate_policy(policies, "sqs:SendMessage", "*") == ALLOW
        assert evaluate_policy(policies, "sqs:DeleteQueue", "*") == DENY
        assert evaluate_policy(policies, "ec2:RunInstances", "*") == IMPLICIT_DENY

    def test_deny_all_except_pattern(self):
        """Deny everything except specific actions."""
        policy = {
            "Statement": [
                {"Effect": "Allow", "Action": "*", "Resource": "*"},
                {
                    "Effect": "Deny",
                    "NotAction": [
                        "s3:GetObject",
                        "s3:ListBucket",
                    ],
                    "Resource": "*",
                },
            ]
        }
        # s3:GetObject is in NotAction, so Deny does NOT apply
        assert evaluate_policy([policy], "s3:GetObject", "*") == ALLOW
        # sqs:SendMessage is NOT in NotAction, so Deny applies
        assert evaluate_policy([policy], "sqs:SendMessage", "*") == DENY

    def test_resource_policy_cross_account_with_condition(self):
        """S3 bucket policy allowing cross-account with IP condition."""
        bucket_policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "arn:aws:iam::999999999999:root"},
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::shared-bucket/*",
                    "Condition": {"IpAddress": {"aws:SourceIp": "203.0.113.0/24"}},
                }
            ]
        }
        ctx = {"aws:SourceIp": "203.0.113.50"}
        assert (
            evaluate_resource_policy(
                bucket_policy,
                "arn:aws:iam::999999999999:root",
                "s3:GetObject",
                "arn:aws:s3:::shared-bucket/file",
                ctx,
            )
            == ALLOW
        )
        bad_ctx = {"aws:SourceIp": "1.2.3.4"}
        assert (
            evaluate_resource_policy(
                bucket_policy,
                "arn:aws:iam::999999999999:root",
                "s3:GetObject",
                "arn:aws:s3:::shared-bucket/file",
                bad_ctx,
            )
            == IMPLICIT_DENY
        )

    def test_condition_with_policy_variable(self):
        """Condition using policy variable in resource."""
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:*",
                    "Resource": "arn:aws:s3:::company-bucket/home/${aws:username}/*",
                }
            ]
        }
        ctx = {"aws:username": "alice"}
        assert (
            evaluate_policy(
                [policy],
                "s3:GetObject",
                "arn:aws:s3:::company-bucket/home/alice/doc.pdf",
                ctx,
            )
            == ALLOW
        )
        assert (
            evaluate_policy(
                [policy],
                "s3:GetObject",
                "arn:aws:s3:::company-bucket/home/bob/doc.pdf",
                ctx,
            )
            == IMPLICIT_DENY
        )


# ===========================================================================
# STS session tests
# ===========================================================================


class TestStsSessions:
    def setup_method(self):
        clear_sts_sessions()

    def test_register_and_clear(self):
        register_sts_session("ASIATEMP", "arn:aws:iam::123:role/admin", "123")
        clear_sts_sessions()
        # After clear, sessions should be empty (tested implicitly)


# ===========================================================================
# Evaluate condition block edge cases
# ===========================================================================


class TestConditionBlockEdgeCases:
    def test_empty_condition_block(self):
        assert evaluate_condition_block({}, {}) is True

    def test_unknown_operator(self):
        result = evaluate_condition_block({"UnknownOp": {"key": "value"}}, {"key": "value"})
        assert result is False

    def test_or_within_values(self):
        """Multiple values for a key means OR."""
        ctx = {"aws:PrincipalTag/env": "staging"}
        result = evaluate_condition_block(
            {"StringEquals": {"aws:PrincipalTag/env": ["production", "staging"]}},
            ctx,
        )
        assert result is True

    def test_and_across_keys(self):
        """Multiple keys under one operator means AND."""
        ctx = {"key1": "a", "key2": "b"}
        # Both must match
        result = evaluate_condition_block(
            {"StringEquals": {"key1": "a", "key2": "WRONG"}},
            ctx,
        )
        assert result is False

    def test_numeric_from_strings(self):
        ctx = {"aws:MultiFactorAuthAge": "3600"}
        result = evaluate_condition_block(
            {"NumericLessThan": {"aws:MultiFactorAuthAge": "7200"}},
            ctx,
        )
        assert result is True


# ===========================================================================
# Additional condition operator edge cases
# ===========================================================================


class TestAdditionalConditionEdgeCases:
    def test_string_like_question_mark(self):
        assert _string_like("abc", "a?c") is True
        assert _string_like("abbc", "a?c") is False

    def test_string_like_star_empty(self):
        assert _string_like("", "*") is True

    def test_ip_address_ipv6(self):
        assert _ip_address("::1", "::1/128") is True

    def test_arn_like_wildcard_in_resource(self):
        assert (
            _arn_like(
                "arn:aws:s3:::my-bucket/path/to/key",
                "arn:aws:s3:::my-bucket/*",
            )
            is True
        )

    def test_date_with_fractional_seconds(self):
        assert _date_equals("2024-01-01T00:00:00.000Z", "2024-01-01T00:00:00.000Z") is True

    def test_bool_with_string_true(self):
        assert _bool_op("True", "true") is True

    def test_numeric_equals_float(self):
        assert _numeric_equals(3.14, "3.14") is True

    def test_for_any_value_string_like(self):
        ctx = {"aws:TagKeys": ["env-prod", "team-infra"]}
        result = evaluate_condition_block(
            {"ForAnyValue:StringLike": {"aws:TagKeys": ["env-*"]}},
            ctx,
        )
        assert result is True

    def test_for_all_values_string_like(self):
        ctx = {"aws:TagKeys": ["env-prod", "env-staging"]}
        result = evaluate_condition_block(
            {"ForAllValues:StringLike": {"aws:TagKeys": ["env-*"]}},
            ctx,
        )
        assert result is True

    def test_for_all_values_string_like_fails(self):
        ctx = {"aws:TagKeys": ["env-prod", "team-infra"]}
        result = evaluate_condition_block(
            {"ForAllValues:StringLike": {"aws:TagKeys": ["env-*"]}},
            ctx,
        )
        assert result is False


# ===========================================================================
# Additional policy engine edge cases
# ===========================================================================


class TestAdditionalPolicyEngineEdgeCases:
    def test_invalid_effect_ignored(self):
        policy = {"Statement": [{"Effect": "Invalid", "Action": "s3:GetObject", "Resource": "*"}]}
        assert evaluate_policy([policy], "s3:GetObject", "*") == IMPLICIT_DENY

    def test_resource_policy_string_principal(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "arn:aws:iam::123456789012:root",
                    "Action": "s3:GetObject",
                    "Resource": "*",
                }
            ]
        }
        assert (
            evaluate_resource_policy(policy, "arn:aws:iam::123456789012:root", "s3:GetObject", "*")
            == ALLOW
        )

    def test_resource_policy_no_principal(self):
        """Statement without Principal should match any caller."""
        policy = {"Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]}
        assert evaluate_resource_policy(policy, "anyone", "s3:GetObject", "*") == ALLOW

    def test_multiple_statements_mixed(self):
        policy = {
            "Statement": [
                {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"},
                {"Effect": "Allow", "Action": "sqs:SendMessage", "Resource": "*"},
                {"Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "*"},
            ]
        }
        assert evaluate_policy([policy], "s3:GetObject", "*") == ALLOW
        assert evaluate_policy([policy], "sqs:SendMessage", "*") == ALLOW
        assert evaluate_policy([policy], "s3:DeleteObject", "*") == DENY
        assert evaluate_policy([policy], "ec2:Describe*", "*") == IMPLICIT_DENY


# ===========================================================================
# Additional middleware edge cases
# ===========================================================================


class TestAdditionalMiddlewareEdgeCases:
    def test_s3_root_path_arn(self):
        request = MagicMock()
        request.url.path = "/"
        request.query_params = {}
        arn = build_resource_arn("s3", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:s3:::*"

    def test_lambda_no_function_in_path(self):
        request = MagicMock()
        request.url.path = "/2015-03-31/other"
        request.query_params = {}
        arn = build_resource_arn("lambda", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:lambda:us-east-1:123456789012:function:*"

    def test_sqs_root_path_no_queue_url(self):
        request = MagicMock()
        request.url.path = "/"
        request.query_params = {}
        arn = build_resource_arn("sqs", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:sqs:us-east-1:123456789012:*"

    def test_sns_no_topic_arn(self):
        request = MagicMock()
        request.url.path = "/"
        request.query_params = {}
        arn = build_resource_arn("sns", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:sns:us-east-1:123456789012:*"

    def test_build_iam_action_kms(self):
        assert build_iam_action("kms", "Encrypt") == "kms:Encrypt"

    def test_build_iam_action_secretsmanager(self):
        assert (
            build_iam_action("secretsmanager", "GetSecretValue") == "secretsmanager:GetSecretValue"
        )


# ===========================================================================
# IAM native provider error-path tests
# ===========================================================================


def _make_iam_request(body: bytes = b"", headers: dict | None = None):
    request = MagicMock()
    request.body = AsyncMock(return_value=body)
    request.headers = headers or {}
    request.method = "POST"
    request.url = MagicMock()
    request.url.path = "/"
    request.url.query = None
    request.query_params = {}
    return request


class TestIAMProviderErrorPaths:
    def test_non_intercepted_action_forwards_to_moto(self):
        """Actions not in _ACTION_MAP (e.g., GetUser) forward to Moto."""
        with patch(
            "robotocore.services.iam.provider.forward_to_moto",
            new_callable=AsyncMock,
        ) as mock_forward:
            mock_forward.return_value = MagicMock(status_code=200, body=b"<ok/>")
            body = b"Action=GetUser&UserName=testuser"
            request = _make_iam_request(body)
            asyncio.run(handle_iam_request(request, "us-east-1", "123456789012"))
            mock_forward.assert_called_once_with(request, "iam", account_id="123456789012")

    def test_moto_nosuchentity_passthrough(self):
        """When Moto returns NoSuchEntity for a non-existent user, it passes through."""
        error_xml = (
            b"<ErrorResponse>"
            b"<Error><Code>NoSuchEntity</Code>"
            b"<Message>The user with name nonexistent cannot be found.</Message>"
            b"</Error></ErrorResponse>"
        )
        with patch(
            "robotocore.services.iam.provider.forward_to_moto",
            new_callable=AsyncMock,
        ) as mock_forward:
            mock_forward.return_value = Response(
                content=error_xml, status_code=404, media_type="text/xml"
            )
            body = b"Action=GetUser&UserName=nonexistent"
            request = _make_iam_request(body)
            resp = asyncio.run(handle_iam_request(request, "us-east-1", "123456789012"))
        assert resp.status_code == 404
        assert b"NoSuchEntity" in resp.body

    def test_moto_entity_already_exists_passthrough(self):
        """When Moto returns EntityAlreadyExists for duplicate creation, it passes through."""
        error_xml = (
            b"<ErrorResponse>"
            b"<Error><Code>EntityAlreadyExists</Code>"
            b"<Message>User with name testuser already exists.</Message>"
            b"</Error></ErrorResponse>"
        )
        with patch(
            "robotocore.services.iam.provider.forward_to_moto",
            new_callable=AsyncMock,
        ) as mock_forward:
            mock_forward.return_value = Response(
                content=error_xml, status_code=409, media_type="text/xml"
            )
            body = b"Action=CreateUser&UserName=testuser"
            request = _make_iam_request(body)
            resp = asyncio.run(handle_iam_request(request, "us-east-1", "123456789012"))
        assert resp.status_code == 409
        assert b"EntityAlreadyExists" in resp.body

    def test_simulate_custom_policy_empty_actions(self):
        """SimulateCustomPolicy with no action names returns empty results."""
        body = b"Action=SimulateCustomPolicy&PolicyInputList.member.1=%7B%7D"
        request = _make_iam_request(body)
        resp = asyncio.run(handle_iam_request(request, "us-east-1", "123456789012"))
        assert resp.status_code == 200
        assert b"<EvaluationResults>" in resp.body
        # No ActionNames.member.N params → no <member> evaluation results
        assert b"<EvalActionName>" not in resp.body

    def test_change_password_always_succeeds(self):
        """ChangePassword is a no-op that always returns 200."""
        body = b"Action=ChangePassword&OldPassword=old&NewPassword=new"
        request = _make_iam_request(body)
        resp = asyncio.run(handle_iam_request(request, "us-east-1", "123456789012"))
        assert resp.status_code == 200
        assert b"ChangePasswordResponse" in resp.body


# ===========================================================================
# Categorical bug tests: error handling on nonexistent entities
# These test patterns apply to ANY native provider that calls Moto backend
# methods directly — the provider must catch Moto exceptions and return
# proper AWS error responses instead of crashing with HTTP 500.
# ===========================================================================


class TestIAMProviderNoSuchEntityHandling:
    """Bug category: Native providers calling Moto backend.get_user() (or similar)
    without catching NoSuchEntity. The provider crashes with 500 instead of
    returning a proper NoSuchEntity XML error response (404)."""

    def test_put_permissions_boundary_nonexistent_user(self):
        """PutUserPermissionsBoundary on a nonexistent user must return NoSuchEntity, not 500."""
        body = (
            b"Action=PutUserPermissionsBoundary"
            b"&UserName=no-such-user"
            b"&PermissionsBoundary=arn:aws:iam::123456789012:policy/SomeBoundary"
        )
        request = _make_iam_request(body)
        resp = asyncio.run(handle_iam_request(request, "us-east-1", "123456789012"))
        assert resp.status_code == 404, f"Expected 404, got {resp.status_code}"
        assert b"NoSuchEntity" in resp.body

    def test_delete_permissions_boundary_nonexistent_user(self):
        """DeleteUserPermissionsBoundary on a nonexistent user must return NoSuchEntity, not 500."""
        body = b"Action=DeleteUserPermissionsBoundary&UserName=no-such-user"
        request = _make_iam_request(body)
        resp = asyncio.run(handle_iam_request(request, "us-east-1", "123456789012"))
        assert resp.status_code == 404, f"Expected 404, got {resp.status_code}"
        assert b"NoSuchEntity" in resp.body

    def test_put_permissions_boundary_missing_username(self):
        """PutUserPermissionsBoundary with empty UserName must return error, not crash."""
        body = (
            b"Action=PutUserPermissionsBoundary"
            b"&PermissionsBoundary=arn:aws:iam::123456789012:policy/SomeBoundary"
        )
        request = _make_iam_request(body)
        resp = asyncio.run(handle_iam_request(request, "us-east-1", "123456789012"))
        # Empty username: Moto raises NoSuchEntity for empty string lookup
        assert resp.status_code in (400, 404), f"Expected 400 or 404, got {resp.status_code}"

    def test_delete_permissions_boundary_missing_username(self):
        """DeleteUserPermissionsBoundary with empty UserName must return error, not crash."""
        body = b"Action=DeleteUserPermissionsBoundary"
        request = _make_iam_request(body)
        resp = asyncio.run(handle_iam_request(request, "us-east-1", "123456789012"))
        assert resp.status_code in (400, 404), f"Expected 400 or 404, got {resp.status_code}"


class TestIAMProviderPermissionsBoundaryInjection:
    """Bug category: Post-processing response injection with overly broad exception
    handling. The bare `except Exception: pass` silently swallows real bugs."""

    def test_getuser_boundary_injection_nonexistent_user(self):
        """GetUser for nonexistent user forwards Moto's 404, doesn't crash in boundary injection."""
        error_xml = (
            b"<ErrorResponse>"
            b"<Error><Code>NoSuchEntity</Code>"
            b"<Message>The user with name ghost cannot be found.</Message>"
            b"</Error></ErrorResponse>"
        )
        with patch(
            "robotocore.services.iam.provider.forward_to_moto",
            new_callable=AsyncMock,
        ) as mock_forward:
            mock_forward.return_value = Response(
                content=error_xml, status_code=404, media_type="text/xml"
            )
            body = b"Action=GetUser&UserName=ghost"
            request = _make_iam_request(body)
            resp = asyncio.run(handle_iam_request(request, "us-east-1", "123456789012"))
        # The boundary injection code should NOT modify error responses
        assert resp.status_code == 404
        assert b"NoSuchEntity" in resp.body

    def test_getuser_boundary_injection_only_runs_on_200(self):
        """Boundary injection should only apply to successful GetUser responses."""
        error_xml = (
            b"<ErrorResponse>"
            b"<Error><Code>ServiceFailure</Code>"
            b"<Message>Internal error</Message>"
            b"</Error></ErrorResponse>"
        )
        with patch(
            "robotocore.services.iam.provider.forward_to_moto",
            new_callable=AsyncMock,
        ) as mock_forward:
            mock_forward.return_value = Response(
                content=error_xml, status_code=500, media_type="text/xml"
            )
            body = b"Action=GetUser&UserName=someone"
            request = _make_iam_request(body)
            resp = asyncio.run(handle_iam_request(request, "us-east-1", "123456789012"))
        # 500 from Moto should pass through unchanged
        assert resp.status_code == 500
        assert b"ServiceFailure" in resp.body
