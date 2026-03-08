"""Failing tests exposing correctness bugs in the IAM policy engine and middleware.

Each test documents a specific bug. Do NOT fix the production code -- only add tests.
"""

from __future__ import annotations

from robotocore.services.iam.conditions import (
    _arn_like,
    evaluate_condition_block,
)
from robotocore.services.iam.policy_engine import (
    ALLOW,
    DENY,
    IMPLICIT_DENY,
    _action_matches,
    _resource_matches,
    evaluate_policy,
    evaluate_resource_policy,
    evaluate_with_permission_boundary,
)

# ===========================================================================
# Bug 1: Permission boundary explicit deny is downgraded to implicit deny
# ===========================================================================


class TestPermissionBoundaryExplicitDeny:
    """When a permission boundary contains an explicit Deny statement that
    matches the action, AWS returns an explicit deny. The current code
    returns IMPLICIT_DENY instead because it only checks whether the
    boundary result != ALLOW, losing the distinction between explicit
    deny and implicit deny.

    AWS docs: "If any of these policies explicitly denies an action,
    the request is denied. This is called an explicit deny."
    https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic.html
    """

    def test_boundary_explicit_deny_should_return_deny_not_implicit_deny(self):
        identity_policy = {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
        permission_boundary = {
            "Statement": [
                {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"},
                {"Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "*"},
            ]
        }
        # Identity allows s3:DeleteObject, but boundary explicitly denies it.
        # AWS would return an explicit DENY, not IMPLICIT_DENY.
        result = evaluate_with_permission_boundary(
            [identity_policy], permission_boundary, "s3:DeleteObject", "*"
        )
        assert result == DENY, (
            f"Expected explicit DENY from permission boundary, got {result}. "
            "The boundary has an explicit Deny statement for s3:DeleteObject."
        )


# ===========================================================================
# Bug 2: NotPrincipal not handled in resource policy evaluation
# ===========================================================================


class TestNotPrincipalHandling:
    """AWS resource policies support NotPrincipal to deny everyone EXCEPT
    a specific principal. The current _principal_matches only checks the
    'Principal' key and ignores 'NotPrincipal', so a NotPrincipal deny
    statement silently matches nobody (the statement is skipped).
    """

    def test_not_principal_deny_should_not_match_excluded_principal(self):
        """The listed NotPrincipal should NOT be denied."""
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-bucket/*",
                },
                {
                    "Effect": "Deny",
                    "NotPrincipal": {"AWS": "arn:aws:iam::123456789012:root"},
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-bucket/*",
                },
            ]
        }
        # The root principal should be allowed (excluded from NotPrincipal deny)
        result = evaluate_resource_policy(
            policy,
            "arn:aws:iam::123456789012:root",
            "s3:GetObject",
            "arn:aws:s3:::my-bucket/secret.txt",
        )
        assert result == ALLOW, (
            f"Expected ALLOW for the excluded principal, got {result}. "
            "The root principal is listed in NotPrincipal and should not be denied."
        )


# ===========================================================================
# Bug 3: fnmatch '?' wildcard accepted in action matching (AWS only has '*')
# ===========================================================================


class TestActionMatchingFnmatchBugs:
    """AWS IAM action matching only supports '*' as a wildcard character.
    The '?' single-character wildcard and '[...]' character classes are
    fnmatch features that should NOT work in IAM policies.

    The current code uses fnmatch.fnmatch() which supports ?, [, ].
    """

    def test_question_mark_should_not_be_wildcard_in_action(self):
        """'s3:Get?bject' should NOT match 's3:GetObject' in real AWS."""
        policy = {"Statement": [{"Effect": "Allow", "Action": "s3:Get?bject", "Resource": "*"}]}
        result = evaluate_policy([policy], "s3:GetObject", "*")
        assert result == IMPLICIT_DENY, (
            f"Expected IMPLICIT_DENY because '?' is not a valid IAM wildcard, got {result}. "
            "AWS IAM only supports '*' wildcards, not '?' single-char wildcards."
        )

    def test_bracket_char_class_should_not_work_in_action(self):
        """'s3:[GP]etObject' should NOT match 's3:GetObject' in real AWS."""
        policy = {"Statement": [{"Effect": "Allow", "Action": "s3:[GP]etObject", "Resource": "*"}]}
        result = evaluate_policy([policy], "s3:GetObject", "*")
        assert result == IMPLICIT_DENY, (
            f"Expected IMPLICIT_DENY because '[...]' is not a valid IAM wildcard, got {result}. "
            "AWS IAM only supports '*' wildcards, not character classes."
        )

    def test_question_mark_in_action_matches_function(self):
        """Direct test of _action_matches with '?' wildcard."""
        # In real AWS, 's3:Get?bject' would be a literal (non-matching) action pattern
        assert _action_matches("s3:GetObject", "s3:Get?bject") is False, (
            "_action_matches should not treat '?' as a wildcard"
        )


# ===========================================================================
# Bug 4: fnmatch '?' wildcard accepted in resource matching
# ===========================================================================


class TestResourceMatchingFnmatchBugs:
    """Same fnmatch issue as actions, but for resource ARN matching."""

    def test_question_mark_should_not_be_wildcard_in_resource(self):
        """'arn:aws:s3:::my-?ucket/*' should NOT match 'arn:aws:s3:::my-bucket/key'."""
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-?ucket/*",
                }
            ]
        }
        result = evaluate_policy([policy], "s3:GetObject", "arn:aws:s3:::my-bucket/key")
        assert result == IMPLICIT_DENY, (
            f"Expected IMPLICIT_DENY because '?' is not a valid IAM resource wildcard, "
            f"got {result}."
        )

    def test_question_mark_in_resource_matches_function(self):
        """Direct test of _resource_matches with '?'."""
        assert _resource_matches("arn:aws:s3:::my-bucket", "arn:aws:s3:::my-?ucket", {}) is False, (
            "_resource_matches should not treat '?' as a wildcard"
        )


# ===========================================================================
# Bug 5: ForAnyValue set operator + IfExists with missing key
# ===========================================================================


class TestForAnyValueIfExistsMissingKey:
    """When a condition uses 'ForAnyValue:StringEqualsIfExists' and
    the condition key is NOT present in the context, the IfExists
    semantics should cause the condition to be satisfied (return True).

    Currently, evaluate_condition_block strips the IfExists suffix and
    passes the base operator to _evaluate_set_operator, but
    _evaluate_set_operator does NOT receive the if_exists flag.
    For ForAnyValue, a missing key returns False, ignoring IfExists.
    """

    def test_for_any_value_if_exists_missing_key_should_pass(self):
        condition_block = {
            "ForAnyValue:StringEqualsIfExists": {"aws:RequestedRegion": ["us-east-1", "eu-west-1"]}
        }
        # Key is absent from context -- IfExists should make condition pass
        result = evaluate_condition_block(condition_block, {})
        assert result is True, (
            "ForAnyValue:StringEqualsIfExists with missing key should be vacuously True "
            "(IfExists semantics), but got False."
        )


# ===========================================================================
# Bug 6: ARN matching with colons in resource segment
# ===========================================================================


class TestArnMatchingColonInResource:
    """ARNs can have colons in the resource portion, e.g.:
    arn:aws:ecs:us-east-1:123456789012:container-instance/cluster/id

    When _arn_match splits by ':', these extra sections get compared
    independently after padding. This produces incorrect matches because
    the padding fills with empty strings, causing a mismatch where there
    should be a match (pattern 'arn:aws:ecs:*:*:container-instance/*'
    should match the ARN but the split creates 7 parts vs 6 parts and
    the padding logic breaks the comparison).
    """

    def test_arn_like_wildcard_resource_vs_colon_resource(self):
        """A wildcard in the 6th segment should match an ARN whose resource
        portion contains additional colon-separated segments."""
        arn = "arn:aws:ecs:us-east-1:123456789012:task:cluster-name:task-id"
        # Pattern with wildcard only in the 6th position (resource type)
        pattern = "arn:aws:ecs:us-east-1:123456789012:task:*"
        # This should match because 'task:cluster-name:task-id' resource
        # starts with 'task:' and '*' should cover the rest
        assert _arn_like(arn, pattern) is True

    def test_arn_like_wildcard_fewer_sections_than_arn(self):
        """Pattern with 6 colon-separated parts, ARN with 8. The 6th part
        of the pattern is '*' which should match everything in the ARN's
        resource section including colons, but _arn_match splits by ':'
        and pads with empty strings, so the 7th and 8th parts of the ARN
        get compared against '' (empty), not against '*'."""
        arn = "arn:aws:states:us-east-1:123456789012:execution:my-sfn:exec-id"
        pattern = "arn:aws:states:us-east-1:123456789012:execution:*"
        # Should match: the wildcard covers 'my-sfn:exec-id'
        # But _arn_match pads pattern to 8 parts: [..., 'execution', '*', '', '']
        # and ARN parts are: [..., 'execution', 'my-sfn', 'exec-id']
        # So '' != 'exec-id' => no match
        assert _arn_like(arn, pattern) is True, (
            "ArnLike pattern with * should match ARN with extra colon-separated segments"
        )
