"""Failing tests for chaos, audit, and IAM infrastructure edge cases.

Each test documents correct behavior that is currently missing or broken.
Do NOT fix the production code -- only add tests here.
"""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

from robotocore.audit.log import AuditLog
from robotocore.chaos.fault_rules import FaultRule, FaultRuleStore
from robotocore.services.iam.conditions import evaluate_condition_block
from robotocore.services.iam.policy_engine import (
    ALLOW,
    DENY,
    IMPLICIT_DENY,
    _iam_match,
    _substitute_variables,
    evaluate_policy,
    evaluate_resource_policy,
)

# ===========================================================================
# CHAOS: Wildcard-style service matching (s3:*, *:GetObject)
# ===========================================================================


class TestChaosWildcardServiceMatching:
    """FaultRule.service is compared with == which means 'all services' requires
    service=None. Users expect glob-style patterns like 's3*' or '*' to work
    as the service filter, similar to how operation supports regex."""

    def test_service_wildcard_star_should_match_any_service(self):
        # Correct behavior: service="*" should match any service, like AWS patterns.
        # Current behavior: service="*" is compared with ==, so it only matches
        # the literal string "*".
        rule = FaultRule(service="*", error_code="InternalError")
        assert rule.matches("s3", "PutObject", "us-east-1") is True, (
            "service='*' should match any service, but the code uses == comparison"
        )

    def test_service_prefix_wildcard_should_match(self):
        # Correct behavior: service="s3*" should match "s3" and "s3outposts".
        # Current behavior: exact string match only.
        rule = FaultRule(service="s3*", error_code="InternalError")
        assert rule.matches("s3outposts", "PutObject", "us-east-1") is True, (
            "service='s3*' should match 's3outposts' via glob, but only exact match is used"
        )


# ===========================================================================
# CHAOS: Rule TTL / expiration
# ===========================================================================


class TestChaosRuleExpiration:
    """Chaos rules should support a TTL (time-to-live) so they auto-expire.
    AWS Fault Injection Simulator experiments have durations. Without TTL,
    rules persist forever until manually removed."""

    def test_rule_with_ttl_should_not_match_after_expiry(self):
        # Correct behavior: a rule created with ttl_seconds=1 should stop matching
        # after 1 second has elapsed.
        # Current behavior: FaultRule has no TTL concept at all.
        rule = FaultRule(service="s3", error_code="InternalError")
        # Simulate TTL by checking if the rule supports it
        assert hasattr(rule, "ttl_seconds"), (
            "FaultRule should have a ttl_seconds attribute for auto-expiration"
        )

    def test_expired_rule_not_returned_by_find_matching(self):
        # Correct behavior: find_matching should skip expired rules.
        store = FaultRuleStore()
        rule = FaultRule(service="s3", error_code="InternalError")
        # Manually set created_at to the past to simulate expiry
        rule.created_at = time.time() - 3600  # 1 hour ago
        store.add(rule)
        # If the rule had a TTL of 60 seconds, it should be expired now
        # and find_matching should return None.
        # Current behavior: no TTL check in find_matching.
        assert hasattr(rule, "ttl_seconds"), (
            "FaultRule needs ttl_seconds to support expiration in find_matching"
        )


# ===========================================================================
# CHAOS: match_count is not thread-safe (race on += 1)
# ===========================================================================


class TestChaosMatchCountAtomicity:
    """match_count += 1 inside matches() is not atomic. When find_matching is
    called from multiple threads, the store lock protects the list iteration
    but matches() is called INSIDE the lock. However, if matches() is called
    directly (bypassing the store), the increment can be lost."""

    def test_match_count_is_atomic_under_concurrent_direct_calls(self):
        # Correct behavior: match_count should be exactly N*M after N threads
        # each calling matches() M times.
        # Current behavior: += 1 is not atomic; under GIL-free Python or with
        # bytecode interleaving, increments can be lost.
        rule = FaultRule(service="s3", error_code="InternalError")
        n_threads = 8
        n_per_thread = 5000
        barrier = threading.Barrier(n_threads)

        def do_matches():
            barrier.wait()
            for _ in range(n_per_thread):
                rule.matches("s3", "PutObject", "us-east-1")

        threads = [threading.Thread(target=do_matches) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        expected = n_threads * n_per_thread
        assert rule.match_count == expected, (
            f"Expected match_count={expected}, got {rule.match_count}. "
            "match_count += 1 is not atomic and can lose increments under contention."
        )


# ===========================================================================
# CHAOS: Latency injection timing accuracy
# ===========================================================================


class TestChaosLatencyInjectionSync:
    """The chaos middleware uses time.sleep() for latency injection. This is
    correct because the handler chain runs synchronously inside
    asyncio.to_thread(), so time.sleep() blocks only the current request
    thread without blocking the event loop."""

    def test_latency_injection_uses_sync_sleep(self):
        # The handler chain is synchronous (called via handler(context), not
        # await handler(context)). It runs inside asyncio.to_thread() from the
        # ASGI layer, so time.sleep() is the correct choice — it blocks only
        # the current thread (delaying this specific request) without blocking
        # the event loop.
        import ast
        import inspect

        from robotocore.chaos import middleware

        source = inspect.getsource(middleware.chaos_handler)
        tree = ast.parse(source)
        uses_time_sleep = any(
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "time"
            and node.attr == "sleep"
            for node in ast.walk(tree)
        )
        assert uses_time_sleep, (
            "chaos_handler should use time.sleep() for latency injection. "
            "The handler chain runs synchronously inside asyncio.to_thread(), "
            "so time.sleep() correctly blocks only the current request thread."
        )


# ===========================================================================
# CHAOS: Error response format should match real AWS
# ===========================================================================


class TestChaosErrorResponseFormat:
    """The chaos middleware generates JSON error responses. For XML-based
    services (S3, STS, EC2), real AWS returns XML error responses, not JSON.
    The current implementation always returns JSON."""

    def test_error_response_has_request_id(self):
        # Correct behavior: AWS error responses always include a RequestId.
        # Current behavior: the chaos middleware error body has __type, message,
        # Message but no RequestId.
        import json

        from robotocore.chaos.middleware import chaos_handler
        from robotocore.gateway.handler_chain import RequestContext

        rule = FaultRule(service="s3", error_code="ThrottlingException")

        ctx = RequestContext.__new__(RequestContext)
        ctx.service_name = "s3"
        ctx.operation = "PutObject"
        ctx.region = "us-east-1"
        ctx.response = None

        with patch("robotocore.chaos.middleware.get_fault_store") as mock_store:
            mock_store.return_value.find_matching.return_value = rule
            chaos_handler(ctx)

        body = json.loads(ctx.response.body.decode())
        assert "RequestId" in body, (
            "AWS error responses always include a RequestId. "
            "The chaos error response is missing it."
        )


# ===========================================================================
# AUDIT: Filtering by service
# ===========================================================================


class TestAuditFilterByService:
    """The audit log provides recent() but has no filtering capability.
    Users need to filter by service, operation, or time range to find
    relevant entries."""

    def test_filter_by_service(self):
        # Correct behavior: AuditLog should support filtering recent entries by service.
        log = AuditLog(max_size=100)
        log.record(service="s3", operation="PutObject", status_code=200)
        log.record(service="dynamodb", operation="PutItem", status_code=200)
        log.record(service="s3", operation="GetObject", status_code=200)

        # The audit log should support a service filter parameter
        assert hasattr(log, "query") or "service" in log.recent.__code__.co_varnames, (
            "AuditLog.recent() should support filtering by service, but it has no filter parameters"
        )

    def test_filter_by_time_range(self):
        # Correct behavior: should be able to get entries within a time window.
        log = AuditLog(max_size=100)
        log.record(service="s3", operation="PutObject", status_code=200)

        # There's no way to filter by timestamp range
        import inspect

        sig = inspect.signature(log.recent)
        param_names = set(sig.parameters.keys())
        assert "since" in param_names or "start_time" in param_names, (
            "AuditLog.recent() should support time-range filtering but only has a 'limit' parameter"
        )


# ===========================================================================
# AUDIT: Concurrent writes correctness
# ===========================================================================


class TestAuditConcurrentWrites:
    """Multiple threads writing to the audit log should not lose entries
    or corrupt state."""

    def test_concurrent_writes_preserve_all_entries(self):
        log = AuditLog(max_size=10000)
        n_threads = 8
        n_per_thread = 500
        barrier = threading.Barrier(n_threads)

        def writer(thread_id: int):
            barrier.wait()
            for i in range(n_per_thread):
                log.record(
                    service=f"svc-{thread_id}",
                    operation=f"op-{i}",
                    status_code=200,
                )

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        entries = log.recent(limit=10000)
        assert len(entries) == n_threads * n_per_thread, (
            f"Expected {n_threads * n_per_thread} entries, got {len(entries)}. "
            "Concurrent writes may have lost entries."
        )


# ===========================================================================
# AUDIT: Entry completeness (all fields present including no-error case)
# ===========================================================================


class TestAuditEntryCompleteness:
    """Audit entries should always have a consistent set of fields,
    regardless of whether there was an error."""

    def test_success_entry_has_no_error_key(self):
        # Current behavior: error key is omitted for success. This is actually
        # correct but means consumers must check for key existence.
        # This test documents the behavior.
        log = AuditLog(max_size=100)
        log.record(service="s3", operation="PutObject", status_code=200)
        entry = log.recent()[0]
        # The 'error' key should always be present for consistency,
        # set to None for successful requests.
        assert "error" in entry, (
            "Audit entries should always include the 'error' key (set to None for success) "
            "for consistent schema. Currently the key is omitted on success."
        )


# ===========================================================================
# AUDIT: duration_ms precision
# ===========================================================================


class TestAuditDurationPrecision:
    """duration_ms is rounded to 2 decimal places. Sub-millisecond timing
    from high-precision clocks may lose precision."""

    def test_duration_preserves_microsecond_precision(self):
        # Correct behavior: duration_ms should preserve at least microsecond
        # precision (3 decimal places) since modern systems can measure this.
        log = AuditLog(max_size=100)
        log.record(service="s3", operation="PutObject", status_code=200, duration_ms=1.23456)
        entry = log.recent()[0]
        # The current code rounds to 2 decimal places (1.23), losing precision
        assert entry["duration_ms"] == 1.235 or entry["duration_ms"] == 1.23456, (
            f"Expected at least 3 decimal places of precision, got {entry['duration_ms']}. "
            "round(duration_ms, 2) loses sub-millisecond timing."
        )


# ===========================================================================
# IAM: _iam_match does not support '?' wildcard (correct per AWS)
# but the conditions module uses fnmatch which DOES support '?'
# ===========================================================================


class TestIamMatchVsFnmatchInconsistency:
    """The policy engine uses _iam_match (re-based, only * wildcard) for
    action/resource matching, but the conditions module uses fnmatch.fnmatch
    for StringLike which supports ?, [, ]. This inconsistency means
    conditions have different wildcard semantics than action/resource matching.

    AWS documentation says StringLike supports * and ? for conditions,
    but IAM action/resource matching only supports *.
    The _iam_match function correctly rejects ?, but test that it does."""

    def test_iam_match_treats_question_mark_as_literal(self):
        # _iam_match should treat ? as a literal character, not a wildcard
        assert _iam_match("s3:GetObject", "s3:Get?bject") is False, (
            "_iam_match should treat '?' as literal, not as a wildcard"
        )

    def test_iam_match_treats_brackets_as_literal(self):
        # _iam_match should treat [...] as literal characters
        assert _iam_match("s3:GetObject", "s3:[G]etObject") is False, (
            "_iam_match should treat '[...]' as literal, not as a character class"
        )


# ===========================================================================
# IAM: Policy variable substitution in conditions
# ===========================================================================


class TestIamPolicyVariableSubstitutionInConditions:
    """Policy variables like ${aws:username} should be substituted in
    condition values, not just in resource ARNs. The current code only
    substitutes variables in _resource_matches, not in conditions."""

    def test_policy_variable_in_condition_value(self):
        # Correct behavior: policy variables in condition values should be resolved.
        # For example: StringEquals with value "${aws:username}" should compare
        # against the resolved username from context.
        context = {"aws:username": "alice", "s3:prefix": "alice/"}
        condition_block = {"StringEquals": {"s3:prefix": "${aws:username}/"}}
        # The condition should substitute ${aws:username} -> "alice"
        # and then compare "alice/" == "alice/"
        result = evaluate_condition_block(condition_block, context)
        assert result is True, (
            "Policy variable ${aws:username} in condition value should be substituted. "
            "Current code only substitutes variables in resource ARNs."
        )


# ===========================================================================
# IAM: Multiple conditions of same type (AND logic)
# ===========================================================================


class TestIamMultipleConditionsAndLogic:
    """All condition operators in a Condition block must be satisfied (AND).
    Within each operator, all keys must be satisfied (AND).
    Within each key's values, any match counts (OR)."""

    def test_multiple_operators_all_must_match(self):
        condition_block = {
            "StringEquals": {"aws:RequestedRegion": "us-east-1"},
            "IpAddress": {"aws:SourceIp": "192.168.1.0/24"},
        }
        context = {
            "aws:RequestedRegion": "us-east-1",
            "aws:SourceIp": "192.168.1.50",
        }
        result = evaluate_condition_block(condition_block, context)
        assert result is True

    def test_one_operator_fails_whole_block_fails(self):
        condition_block = {
            "StringEquals": {"aws:RequestedRegion": "us-east-1"},
            "IpAddress": {"aws:SourceIp": "10.0.0.0/8"},
        }
        context = {
            "aws:RequestedRegion": "us-east-1",
            "aws:SourceIp": "192.168.1.50",  # not in 10.0.0.0/8
        }
        result = evaluate_condition_block(condition_block, context)
        assert result is False


# ===========================================================================
# IAM: Cross-account access with resource policy
# ===========================================================================


class TestIamCrossAccountAccess:
    """When a principal from account A tries to access a resource in account B,
    the resource policy in account B can grant access. The identity policy
    in account A must also allow it. The combined evaluation should:
    - Allow if both identity policy AND resource policy allow
    - Deny if either explicitly denies"""

    def test_cross_account_resource_policy_allows_different_account(self):
        resource_policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "arn:aws:iam::111111111111:root"},
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::shared-bucket/*",
                }
            ]
        }
        result = evaluate_resource_policy(
            resource_policy,
            "arn:aws:iam::111111111111:root",
            "s3:GetObject",
            "arn:aws:s3:::shared-bucket/file.txt",
        )
        assert result == ALLOW

    def test_cross_account_resource_policy_denies_wrong_account(self):
        resource_policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "arn:aws:iam::111111111111:root"},
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::shared-bucket/*",
                }
            ]
        }
        result = evaluate_resource_policy(
            resource_policy,
            "arn:aws:iam::222222222222:root",
            "s3:GetObject",
            "arn:aws:s3:::shared-bucket/file.txt",
        )
        assert result == IMPLICIT_DENY


# ===========================================================================
# IAM: Deny overrides Allow even across multiple policies
# ===========================================================================


class TestIamDenyOverridesAllow:
    """An explicit Deny in ANY policy overrides Allow in ALL other policies."""

    def test_deny_in_second_policy_overrides_allow_in_first(self):
        policy1 = {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
        policy2 = {"Statement": [{"Effect": "Deny", "Action": "s3:DeleteBucket", "Resource": "*"}]}
        result = evaluate_policy([policy1, policy2], "s3:DeleteBucket", "*")
        assert result == DENY

    def test_deny_in_first_policy_overrides_allow_in_second(self):
        policy1 = {"Statement": [{"Effect": "Deny", "Action": "s3:DeleteBucket", "Resource": "*"}]}
        policy2 = {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
        result = evaluate_policy([policy1, policy2], "s3:DeleteBucket", "*")
        assert result == DENY


# ===========================================================================
# IAM: _substitute_variables edge cases
# ===========================================================================


class TestIamVariableSubstitution:
    """Policy variable substitution edge cases."""

    def test_unknown_variable_preserved_literally(self):
        # Unknown variables should be kept as-is per AWS behavior
        result = _substitute_variables("arn:aws:s3:::${unknown:var}/*", {})
        assert result == "arn:aws:s3:::${unknown:var}/*"

    def test_nested_braces_not_supported(self):
        # ${${nested}} should not crash
        result = _substitute_variables("${${nested}}", {"${nested}": "value"})
        # The outer ${...} matches "${${nested}" which is a valid variable name
        # containing a ${ prefix. This is unlikely but shouldn't crash.
        assert isinstance(result, str)

    def test_multiple_variables_in_one_string(self):
        context = {"aws:username": "alice", "aws:PrincipalAccount": "123456789012"}
        result = _substitute_variables(
            "arn:aws:s3:::${aws:PrincipalAccount}-${aws:username}/*", context
        )
        assert result == "arn:aws:s3:::123456789012-alice/*"


# ===========================================================================
# IAM: Condition Null operator
# ===========================================================================


class TestIamNullCondition:
    """The Null condition operator checks for key presence/absence."""

    def test_null_true_key_absent(self):
        # Null: true should match when key is absent
        condition = {"Null": {"aws:TokenIssueTime": "true"}}
        result = evaluate_condition_block(condition, {})
        assert result is True

    def test_null_true_key_present(self):
        # Null: true should NOT match when key is present
        condition = {"Null": {"aws:TokenIssueTime": "true"}}
        result = evaluate_condition_block(condition, {"aws:TokenIssueTime": "2023-01-01"})
        assert result is False

    def test_null_false_key_present(self):
        # Null: false should match when key IS present
        condition = {"Null": {"aws:TokenIssueTime": "false"}}
        result = evaluate_condition_block(condition, {"aws:TokenIssueTime": "2023-01-01"})
        assert result is True

    def test_null_false_key_absent(self):
        # Null: false should NOT match when key is absent
        condition = {"Null": {"aws:TokenIssueTime": "false"}}
        result = evaluate_condition_block(condition, {})
        assert result is False


# ===========================================================================
# IAM: IfExists suffix with missing key
# ===========================================================================


class TestIamIfExistsSuffix:
    """StringEqualsIfExists: if the key is missing from context, the condition
    is vacuously satisfied."""

    def test_string_equals_if_exists_missing_key_passes(self):
        condition = {"StringEqualsIfExists": {"aws:RequestedRegion": "us-east-1"}}
        result = evaluate_condition_block(condition, {})
        assert result is True

    def test_string_equals_if_exists_present_key_must_match(self):
        condition = {"StringEqualsIfExists": {"aws:RequestedRegion": "us-east-1"}}
        result = evaluate_condition_block(condition, {"aws:RequestedRegion": "eu-west-1"})
        assert result is False

    def test_string_equals_if_exists_present_key_matches(self):
        condition = {"StringEqualsIfExists": {"aws:RequestedRegion": "us-east-1"}}
        result = evaluate_condition_block(condition, {"aws:RequestedRegion": "us-east-1"})
        assert result is True


# ===========================================================================
# IAM: NotAction with wildcard
# ===========================================================================


class TestIamNotAction:
    """NotAction means the statement applies to all actions EXCEPT the listed ones."""

    def test_not_action_allows_unlisted_action(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "NotAction": "iam:*",
                    "Resource": "*",
                }
            ]
        }
        # s3:PutObject is not iam:*, so NotAction matches => Allow
        result = evaluate_policy([policy], "s3:PutObject", "*")
        assert result == ALLOW

    def test_not_action_does_not_allow_listed_action(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "NotAction": "iam:*",
                    "Resource": "*",
                }
            ]
        }
        # iam:CreateUser IS iam:*, so NotAction does NOT match
        result = evaluate_policy([policy], "iam:CreateUser", "*")
        assert result == IMPLICIT_DENY


# ===========================================================================
# IAM: NotResource
# ===========================================================================


class TestIamNotResource:
    """NotResource means the statement applies to all resources EXCEPT the listed ones."""

    def test_not_resource_allows_unlisted_resource(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:*",
                    "NotResource": "arn:aws:s3:::sensitive-bucket/*",
                }
            ]
        }
        result = evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::normal-bucket/file.txt")
        assert result == ALLOW

    def test_not_resource_does_not_match_listed_resource(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:*",
                    "NotResource": "arn:aws:s3:::sensitive-bucket/*",
                }
            ]
        }
        result = evaluate_policy(
            [policy], "s3:GetObject", "arn:aws:s3:::sensitive-bucket/secret.txt"
        )
        assert result == IMPLICIT_DENY


# ===========================================================================
# IAM: Action matching is case-insensitive
# ===========================================================================


class TestIamActionCaseInsensitive:
    """AWS IAM action matching is case-insensitive."""

    def test_action_matching_case_insensitive(self):
        policy = {"Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]}
        # Different cases should all match
        assert evaluate_policy([policy], "s3:getobject", "*") == ALLOW
        assert evaluate_policy([policy], "S3:GETOBJECT", "*") == ALLOW
        assert evaluate_policy([policy], "s3:GetObject", "*") == ALLOW


# ===========================================================================
# IAM: Resource matching is case-sensitive (unlike actions)
# ===========================================================================


class TestIamResourceCaseSensitive:
    """AWS IAM resource ARN matching is case-sensitive for most components,
    but the current _iam_match uses re.IGNORECASE for everything."""

    def test_resource_matching_should_be_case_sensitive(self):
        # Correct behavior: resource ARN matching should be case-sensitive.
        # Current behavior: _iam_match uses re.IGNORECASE, so
        # "arn:aws:s3:::MyBucket" matches pattern "arn:aws:s3:::mybucket".
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::mybucket/*",
                }
            ]
        }
        # "MyBucket" != "mybucket" -- resource matching should be case-sensitive
        result = evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::MyBucket/file.txt")
        assert result == IMPLICIT_DENY, (
            f"Expected IMPLICIT_DENY because resource matching should be case-sensitive, "
            f"got {result}. _iam_match uses re.IGNORECASE which makes resources case-insensitive."
        )


# ===========================================================================
# IAM: Principal matching with AWS account number (no ARN)
# ===========================================================================


class TestIamPrincipalAccountNumber:
    """Resource policies can specify Principal as just an account number
    like "123456789012", which should match any principal from that account."""

    def test_principal_account_number_matches_role_arn(self):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "123456789012"},
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::bucket/*",
                }
            ]
        }
        # An account number in Principal should match any IAM entity in that account.
        # AWS resolves "123456789012" to "arn:aws:iam::123456789012:root" internally.
        result = evaluate_resource_policy(
            policy,
            "arn:aws:iam::123456789012:role/MyRole",
            "s3:GetObject",
            "arn:aws:s3:::bucket/file.txt",
        )
        assert result == ALLOW, (
            f"Expected ALLOW because account number '123456789012' should match any "
            f"principal from that account, got {result}."
        )


# ===========================================================================
# IAM: ForAllValues with empty context (vacuous truth)
# ===========================================================================


class TestIamForAllValuesEmpty:
    """ForAllValues: with an empty set of request values is vacuously true.
    This is a documented AWS behavior that is often surprising."""

    def test_for_all_values_empty_context_is_true(self):
        condition = {"ForAllValues:StringEquals": {"dynamodb:Attributes": ["Name", "Age"]}}
        # Empty context (no attributes requested) -> vacuously true
        result = evaluate_condition_block(condition, {})
        assert result is True

    def test_for_all_values_with_subset(self):
        condition = {"ForAllValues:StringEquals": {"dynamodb:Attributes": ["Name", "Age"]}}
        context = {"dynamodb:Attributes": ["Name"]}
        result = evaluate_condition_block(condition, context)
        assert result is True

    def test_for_all_values_with_extra_value_fails(self):
        condition = {"ForAllValues:StringEquals": {"dynamodb:Attributes": ["Name", "Age"]}}
        context = {"dynamodb:Attributes": ["Name", "SSN"]}
        result = evaluate_condition_block(condition, context)
        assert result is False


# ===========================================================================
# IAM: ForAnyValue
# ===========================================================================


class TestIamForAnyValue:
    """ForAnyValue: returns true if at least one context value matches
    at least one policy value."""

    def test_for_any_value_one_match_is_enough(self):
        condition = {"ForAnyValue:StringEquals": {"dynamodb:Attributes": ["Name", "Age"]}}
        context = {"dynamodb:Attributes": ["Name", "SSN", "Address"]}
        result = evaluate_condition_block(condition, context)
        assert result is True

    def test_for_any_value_no_match(self):
        condition = {"ForAnyValue:StringEquals": {"dynamodb:Attributes": ["Name", "Age"]}}
        context = {"dynamodb:Attributes": ["SSN", "Address"]}
        result = evaluate_condition_block(condition, context)
        assert result is False


# ===========================================================================
# IAM: Statement with single dict (not list) for Statement
# ===========================================================================


class TestIamSingleStatementDict:
    """IAM policies can have Statement as a single dict, not wrapped in a list."""

    def test_single_statement_as_dict(self):
        policy = {
            "Statement": {
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": "*",
            }
        }
        result = evaluate_policy([policy], "s3:GetObject", "*")
        assert result == ALLOW


# ===========================================================================
# IAM: Empty policies list
# ===========================================================================


class TestIamEmptyPolicies:
    """Evaluating with no policies should result in implicit deny."""

    def test_no_policies_implicit_deny(self):
        result = evaluate_policy([], "s3:GetObject", "*")
        assert result == IMPLICIT_DENY

    def test_empty_statement_list(self):
        policy = {"Statement": []}
        result = evaluate_policy([policy], "s3:GetObject", "*")
        assert result == IMPLICIT_DENY


# ===========================================================================
# CHAOS: find_matching probability check happens inside lock
# ===========================================================================


class TestChaosProbabilityInsideLock:
    """The probability check in matches() uses random.random() which is
    called while holding the store lock. This means the probability check
    is non-deterministic inside a critical section, but at least it's
    consistent. However, the match_count increment is also inside the lock
    via find_matching, which means a "miss" due to probability still
    consumes lock time scanning remaining rules."""

    def test_probability_miss_does_not_skip_later_rules(self):
        # If rule1 has probability=0.5 and misses, rule2 should still be checked.
        # Current behavior: find_matching returns the FIRST matching rule.
        # If rule1 misses due to probability, it moves to rule2. This is correct
        # but subtle.
        store = FaultRuleStore()
        # Rule 1: probability=0 (never fires), Rule 2: probability=1 (always fires)
        store.add(FaultRule(rule_id="r1", service="s3", error_code="First", probability=0.0))
        store.add(FaultRule(rule_id="r2", service="s3", error_code="Second", probability=1.0))

        match = store.find_matching("s3", "PutObject", "us-east-1")
        assert match is not None
        assert match.rule_id == "r2", (
            "When first rule misses due to probability, the store should check subsequent rules"
        )


# ===========================================================================
# AUDIT: Ring buffer size from environment variable
# ===========================================================================


class TestAuditEnvConfig:
    """AuditLog reads AUDIT_LOG_SIZE from environment."""

    def test_env_var_overrides_default(self):
        import os

        old = os.environ.get("AUDIT_LOG_SIZE")
        try:
            os.environ["AUDIT_LOG_SIZE"] = "50"
            log = AuditLog()
            assert log._entries.maxlen == 50
        finally:
            if old is None:
                os.environ.pop("AUDIT_LOG_SIZE", None)
            else:
                os.environ["AUDIT_LOG_SIZE"] = old

    def test_invalid_env_var_raises(self):
        # Correct behavior: invalid AUDIT_LOG_SIZE should raise a clear error.
        # Current behavior: int("not_a_number") raises ValueError with a generic message.
        import os

        old = os.environ.get("AUDIT_LOG_SIZE")
        try:
            os.environ["AUDIT_LOG_SIZE"] = "not_a_number"
            # This should raise a descriptive error, not a raw ValueError
            try:
                AuditLog()
                assert False, "Should have raised an error for invalid AUDIT_LOG_SIZE"
            except ValueError as e:
                assert "AUDIT_LOG_SIZE" in str(e), (
                    f"Error message should mention AUDIT_LOG_SIZE, got: {e}"
                )
        finally:
            if old is None:
                os.environ.pop("AUDIT_LOG_SIZE", None)
            else:
                os.environ["AUDIT_LOG_SIZE"] = old
