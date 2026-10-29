"""Tests for IAM policy simulation (SimulateCustomPolicy / SimulatePrincipalPolicy logic)."""

from __future__ import annotations

import json
import uuid

import pytest
from moto.iam.models import iam_backends

from robotocore.services.iam.simulator import (
    ALLOWED,
    EXPLICIT_DENY,
    IMPLICIT_DENY,
    ContextValues,
    InvalidPrincipalArnError,
    PrincipalNotFoundError,
    SourcePolicy,
    account_of,
    collect_principal_policies,
    load_policy_document,
    simulate,
)

BUCKET_OBJECTS = "arn:aws:s3:::example-bucket/*"
OBJECT_ARN = "arn:aws:s3:::example-bucket/report.csv"


def _policy(*statements: dict) -> dict:
    return {"Version": "2012-10-17", "Statement": list(statements)}


def _allow(action: str | list[str], resource: str | list[str] = "*", **extra) -> dict:
    return {"Effect": "Allow", "Action": action, "Resource": resource, **extra}


def _deny(action: str | list[str], resource: str | list[str] = "*", **extra) -> dict:
    return {"Effect": "Deny", "Action": action, "Resource": resource, **extra}


def _src(policy_id: str, *statements: dict) -> SourcePolicy:
    return SourcePolicy(policy_id, _policy(*statements))


def _sim(identity: list[SourcePolicy], action: str, resource: str = "*", **kwargs):
    context = kwargs.pop("context", ContextValues())
    return simulate(identity=identity, action=action, resource=resource, context=context, **kwargs)


class TestDecisions:
    def test_allow_reports_matching_statement(self):
        result = _sim([_src("PolicyInputList.1", _allow("s3:GetObject"))], "s3:GetObject")
        assert result.decision == ALLOWED
        assert [p.policy_id for p in result.matched] == ["PolicyInputList.1"]
        assert result.matched[0].policy_type == "IAM Policy"
        assert result.missing_context == []
        assert result.allowed_by_boundary is None

    def test_no_matching_statement_is_implicit_deny(self):
        result = _sim([_src("PolicyInputList.1", _allow("s3:GetObject"))], "s3:PutObject")
        assert result.decision == IMPLICIT_DENY
        assert result.matched == []

    def test_no_policies_is_implicit_deny(self):
        assert _sim([], "s3:GetObject").decision == IMPLICIT_DENY

    def test_explicit_deny_beats_allow_in_another_policy(self):
        identity = [
            _src("PolicyInputList.1", _allow("s3:*")),
            _src("PolicyInputList.2", _deny("s3:DeleteObject")),
        ]
        result = _sim(identity, "s3:DeleteObject")
        assert result.decision == EXPLICIT_DENY
        assert [p.policy_id for p in result.matched] == ["PolicyInputList.2"]
        assert _sim(identity, "s3:GetObject").decision == ALLOWED

    def test_each_matching_allow_statement_is_listed(self):
        identity = [_src("PolicyInputList.1", _allow("s3:GetObject"), _allow("s3:Get*"))]
        result = _sim(identity, "s3:GetObject")
        assert [p.policy_id for p in result.matched] == ["PolicyInputList.1"] * 2

    def test_statement_without_effect_is_ignored(self):
        identity = [_src("p", {"Action": "s3:GetObject", "Resource": "*"})]
        assert _sim(identity, "s3:GetObject").decision == IMPLICIT_DENY


class TestMatching:
    def test_not_action(self):
        identity = [_src("p", {"Effect": "Allow", "NotAction": "iam:*", "Resource": "*"})]
        assert _sim(identity, "s3:GetObject").decision == ALLOWED
        assert _sim(identity, "iam:CreateUser").decision == IMPLICIT_DENY

    def test_not_resource_deny(self):
        identity = [
            _src(
                "p",
                _allow("s3:GetObject"),
                {"Effect": "Deny", "Action": "s3:GetObject", "NotResource": BUCKET_OBJECTS},
            )
        ]
        assert _sim(identity, "s3:GetObject", OBJECT_ARN).decision == ALLOWED
        other = "arn:aws:s3:::other-bucket/x"
        assert _sim(identity, "s3:GetObject", other).decision == EXPLICIT_DENY

    def test_star_and_question_mark_wildcards(self):
        identity = [_src("p", _allow("ec2:Describe*"), _allow("s3:?etObject", BUCKET_OBJECTS))]
        assert _sim(identity, "ec2:DescribeInstances").decision == ALLOWED
        assert _sim(identity, "s3:GetObject", OBJECT_ARN).decision == ALLOWED
        assert _sim(identity, "s3:PutObject", OBJECT_ARN).decision == IMPLICIT_DENY

    def test_resource_specific_statement_does_not_match_star(self):
        """With no ResourceArns the simulated resource is the literal "*"."""
        identity = [_src("p", _allow("s3:GetObject", BUCKET_OBJECTS))]
        assert _sim(identity, "s3:GetObject").decision == IMPLICIT_DENY
        assert _sim(identity, "s3:GetObject", OBJECT_ARN).decision == ALLOWED


class TestConditions:
    STATEMENT = _allow(
        "s3:GetObject", Condition={"StringEquals": {"aws:RequestedRegion": "us-east-1"}}
    )

    def test_condition_met(self):
        context = ContextValues({"aws:RequestedRegion": "us-east-1"})
        result = _sim([_src("p", self.STATEMENT)], "s3:GetObject", context=context)
        assert result.decision == ALLOWED
        assert result.missing_context == []

    def test_condition_not_met(self):
        context = ContextValues({"aws:RequestedRegion": "eu-west-1"})
        result = _sim([_src("p", self.STATEMENT)], "s3:GetObject", context=context)
        assert result.decision == IMPLICIT_DENY
        assert result.missing_context == []

    def test_missing_key_is_reported_and_condition_fails(self):
        result = _sim([_src("p", self.STATEMENT)], "s3:GetObject")
        assert result.decision == IMPLICIT_DENY
        assert result.missing_context == ["aws:RequestedRegion"]

    def test_missing_keys_only_reported_for_matching_statements(self):
        identity = [_src("p", self.STATEMENT)]
        assert _sim(identity, "s3:PutObject").missing_context == []

    def test_context_keys_are_case_insensitive(self):
        context = ContextValues({"AWS:REQUESTEDREGION": "us-east-1"})
        result = _sim([_src("p", self.STATEMENT)], "s3:GetObject", context=context)
        assert result.decision == ALLOWED

    def test_policy_variable_in_resource(self):
        statement = _allow("s3:GetObject", "arn:aws:s3:::example-bucket/home/${aws:username}/*")
        resource = "arn:aws:s3:::example-bucket/home/alice/notes.txt"
        context = ContextValues({"aws:username": "alice"})
        assert _sim([_src("p", statement)], "s3:GetObject", resource, context=context).decision == (
            ALLOWED
        )
        unset = _sim([_src("p", statement)], "s3:GetObject", resource)
        assert unset.decision == IMPLICIT_DENY
        assert unset.missing_context == ["aws:username"]

    def test_multivalued_context_with_set_operator(self):
        statement = _allow(
            "s3:GetObject",
            Condition={"ForAllValues:StringEquals": {"aws:TagKeys": ["team", "env"]}},
        )
        ok = ContextValues({"aws:TagKeys": ["team"]})
        bad = ContextValues({"aws:TagKeys": ["team", "owner"]})
        assert _sim([_src("p", statement)], "s3:GetObject", context=ok).decision == ALLOWED
        assert _sim([_src("p", statement)], "s3:GetObject", context=bad).decision == IMPLICIT_DENY

    def test_deny_with_negated_operator_applies_when_key_missing(self):
        identity = [
            _src(
                "p",
                _allow("s3:GetObject"),
                _deny(
                    "s3:GetObject",
                    Condition={"StringNotEquals": {"aws:PrincipalTag/team": "data"}},
                ),
            )
        ]
        assert _sim(identity, "s3:GetObject").decision == EXPLICIT_DENY
        tagged = ContextValues({"aws:PrincipalTag/team": "data"})
        assert _sim(identity, "s3:GetObject", context=tagged).decision == ALLOWED

    def test_unsupported_operator_never_grants(self):
        statement = _allow("s3:GetObject", Condition={"NoSuchOperator": {"aws:SourceIp": "x"}})
        context = ContextValues({"aws:SourceIp": "x"})
        result = _sim([_src("p", statement)], "s3:GetObject", context=context)
        assert result.decision == IMPLICIT_DENY

    def test_unsupported_operator_on_deny_still_denies(self):
        identity = [
            _src(
                "p",
                _allow("s3:GetObject"),
                _deny("s3:GetObject", Condition={"NoSuchOperator": {"aws:SourceIp": "x"}}),
            )
        ]
        context = ContextValues({"aws:SourceIp": "x"})
        assert _sim(identity, "s3:GetObject", context=context).decision == EXPLICIT_DENY


class TestPermissionsBoundary:
    IDENTITY = [_src("PolicyInputList.1", _allow(["s3:*", "ec2:*"]))]
    BOUNDARY = [_src("PermissionsBoundaryPolicyInputList.1", _allow("s3:*"))]

    def test_boundary_limits_allow(self):
        result = _sim(self.IDENTITY, "ec2:RunInstances", boundary=self.BOUNDARY)
        assert result.decision == IMPLICIT_DENY
        assert result.allowed_by_boundary is False

    def test_allowed_by_both(self):
        result = _sim(self.IDENTITY, "s3:GetObject", boundary=self.BOUNDARY)
        assert result.decision == ALLOWED
        assert result.allowed_by_boundary is True
        assert [p.policy_id for p in result.matched] == [
            "PolicyInputList.1",
            "PermissionsBoundaryPolicyInputList.1",
        ]

    def test_boundary_alone_grants_nothing(self):
        result = _sim([], "s3:GetObject", boundary=self.BOUNDARY)
        assert result.decision == IMPLICIT_DENY
        assert result.allowed_by_boundary is True

    def test_deny_in_boundary_is_explicit(self):
        boundary = [_src("b", _allow("*"), _deny("s3:DeleteBucket"))]
        result = _sim(self.IDENTITY, "s3:DeleteBucket", boundary=boundary)
        assert result.decision == EXPLICIT_DENY
        assert result.allowed_by_boundary is False

    def test_empty_boundary_denies_everything(self):
        assert _sim(self.IDENTITY, "s3:GetObject", boundary=[]).decision == IMPLICIT_DENY


class TestResourcePolicy:
    USER = "arn:aws:iam::123456789012:user/alice"
    ROLE = "arn:aws:iam::123456789012:role/app"

    @staticmethod
    def _bucket_policy(principal: str, effect: str = "Allow") -> SourcePolicy:
        return SourcePolicy(
            "ResourcePolicy",
            _policy(
                {
                    "Effect": effect,
                    "Principal": {"AWS": principal},
                    "Action": "s3:GetObject",
                    "Resource": BUCKET_OBJECTS,
                }
            ),
            "resource",
        )

    def test_same_account_resource_policy_grants_alone(self):
        result = _sim(
            [],
            "s3:GetObject",
            OBJECT_ARN,
            resource_policy=self._bucket_policy(self.USER),
            caller_arn=self.USER,
        )
        assert result.decision == ALLOWED
        assert [(p.policy_id, p.policy_type) for p in result.matched] == [
            ("ResourcePolicy", "resource")
        ]

    def test_resource_policy_for_other_principal_does_not_apply(self):
        result = _sim(
            [],
            "s3:GetObject",
            OBJECT_ARN,
            resource_policy=self._bucket_policy("arn:aws:iam::123456789012:user/bob"),
            caller_arn=self.USER,
        )
        assert result.decision == IMPLICIT_DENY

    def test_resource_policy_deny_wins(self):
        identity = [_src("p", _allow("s3:*"))]
        result = _sim(
            identity,
            "s3:GetObject",
            OBJECT_ARN,
            resource_policy=self._bucket_policy(self.USER, "Deny"),
            caller_arn=self.USER,
        )
        assert result.decision == EXPLICIT_DENY

    def test_cross_account_needs_both_sides(self):
        identity = [_src("p", _allow("s3:GetObject"))]
        policy = self._bucket_policy(self.USER)
        kwargs = {"caller_arn": self.USER, "cross_account": True}
        assert _sim(identity, "s3:GetObject", OBJECT_ARN, **kwargs).decision == IMPLICIT_DENY
        assert _sim([], "s3:GetObject", OBJECT_ARN, resource_policy=policy, **kwargs).decision == (
            IMPLICIT_DENY
        )
        both = _sim(identity, "s3:GetObject", OBJECT_ARN, resource_policy=policy, **kwargs)
        assert both.decision == ALLOWED

    def test_boundary_limits_resource_grant_to_role_but_not_user(self):
        boundary = [_src("b", _allow("ec2:*"))]
        for caller, expected in ((self.ROLE, IMPLICIT_DENY), (self.USER, ALLOWED)):
            result = _sim(
                [],
                "s3:GetObject",
                OBJECT_ARN,
                boundary=boundary,
                resource_policy=self._bucket_policy(caller),
                caller_arn=caller,
            )
            assert result.decision == expected, caller


class TestHelpers:
    def test_account_of(self):
        assert account_of("123456789012") == "123456789012"
        assert account_of("arn:aws:iam::111122223333:root") == "111122223333"
        assert account_of("") is None
        assert account_of("not-an-arn") is None

    def test_load_policy_document_accepts_url_encoded(self):
        doc = _policy(_allow("s3:GetObject"))
        from urllib.parse import quote

        assert load_policy_document(quote(json.dumps(doc))) == doc

    def test_load_policy_document_rejects_non_objects(self):
        with pytest.raises(ValueError):
            load_policy_document("[1, 2]")
        with pytest.raises(ValueError):
            load_policy_document("{not json")


def _account() -> str:
    return str(uuid.uuid4().int)[:12].zfill(12)


def _doc(*statements: dict) -> str:
    return json.dumps(_policy(*statements))


class TestCollectPrincipalPolicies:
    @pytest.fixture
    def env(self):
        account = _account()
        backend = iam_backends[account]["aws"]
        managed = backend.create_policy(
            description="",
            path="/",
            policy_document=_doc(_allow("sqs:SendMessage")),
            policy_name="QueueWriter",
            tags=[],
        )
        return account, backend, managed

    @staticmethod
    def _resolve(backend):
        return lambda arn: backend.managed_policies.get(arn)

    def test_user_with_inline_managed_and_group_policies(self, env):
        account, backend, managed = env
        backend.create_user("us-east-1", "alice")
        backend.put_user_policy("alice", "ReadReports", _doc(_allow("s3:GetObject")))
        backend.attach_user_policy(managed.arn, "alice")
        backend.create_group("analysts")
        backend.put_group_policy("analysts", "DescribeAll", _doc(_allow("ec2:Describe*")))
        backend.add_user_to_group("analysts", "alice")
        backend.create_group("unrelated")
        backend.put_group_policy("unrelated", "Admin", _doc(_allow("*")))

        identity, boundary = collect_principal_policies(
            backend, f"arn:aws:iam::{account}:user/alice", account, self._resolve(backend)
        )
        assert [p.policy_id for p in identity] == ["ReadReports", "QueueWriter", "DescribeAll"]
        assert boundary is None

    def test_user_boundary_is_resolved(self, env):
        account, backend, managed = env
        user = backend.create_user("us-east-1", "bob")
        user.permissions_boundary_arn = managed.arn
        _identity, boundary = collect_principal_policies(
            backend, f"arn:aws:iam::{account}:user/bob", account, self._resolve(backend)
        )
        assert boundary is not None
        assert [p.policy_id for p in boundary] == ["QueueWriter"]

    def test_unresolvable_boundary_allows_nothing(self, env):
        account, backend, _managed = env
        user = backend.create_user("us-east-1", "carol")
        user.permissions_boundary_arn = f"arn:aws:iam::{account}:policy/Gone"
        _identity, boundary = collect_principal_policies(
            backend, f"arn:aws:iam::{account}:user/carol", account, self._resolve(backend)
        )
        assert boundary == []

    def test_group(self, env):
        account, backend, managed = env
        backend.create_group("ops", "/teams/")
        backend.put_group_policy("ops", "Restart", _doc(_allow("ec2:RebootInstances")))
        backend.attach_group_policy(managed.arn, "ops")
        identity, boundary = collect_principal_policies(
            backend, f"arn:aws:iam::{account}:group/teams/ops", account, self._resolve(backend)
        )
        assert [p.policy_id for p in identity] == ["Restart", "QueueWriter"]
        assert boundary is None

    def test_role_with_boundary(self, env):
        account, backend, managed = env
        backend.create_role(
            role_name="app",
            assume_role_policy_document=_doc(),
            path="/service/",
            permissions_boundary=managed.arn,
            description="",
            tags=[],
            max_session_duration=None,
        )
        backend.put_role_policy("app", "Inline", _doc(_allow("s3:*")))
        identity, boundary = collect_principal_policies(
            backend, f"arn:aws:iam::{account}:role/service/app", account, self._resolve(backend)
        )
        assert [p.policy_id for p in identity] == ["Inline"]
        assert boundary is not None and [p.policy_id for p in boundary] == ["QueueWriter"]

    @pytest.mark.parametrize("kind", ["user", "group", "role"])
    def test_missing_principal(self, env, kind):
        account, backend, _managed = env
        with pytest.raises(PrincipalNotFoundError, match=f"The {kind} with name ghost"):
            collect_principal_policies(
                backend, f"arn:aws:iam::{account}:{kind}/ghost", account, self._resolve(backend)
            )

    def test_principal_in_another_account_is_not_found(self, env):
        account, backend, _managed = env
        backend.create_user("us-east-1", "dave")
        with pytest.raises(PrincipalNotFoundError):
            collect_principal_policies(
                backend, "arn:aws:iam::111122223333:user/dave", account, self._resolve(backend)
            )

    @pytest.mark.parametrize(
        "arn", ["", "alice", "arn:aws:s3:::example-bucket", "arn:aws:iam::123456789012:policy/x"]
    )
    def test_invalid_arn(self, env, arn):
        account, backend, _managed = env
        with pytest.raises(InvalidPrincipalArnError):
            collect_principal_policies(backend, arn, account, self._resolve(backend))
