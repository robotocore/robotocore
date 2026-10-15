"""IAM policy simulation for SimulateCustomPolicy and SimulatePrincipalPolicy.

Builds on the statement-matching helpers in ``policy_engine`` but keeps track of
which statement in which policy decided each result, because the simulator
reports that (MatchedStatements) along with the condition keys a policy needed
but the caller did not supply (MissingContextValues).
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote

from robotocore.services.iam.conditions import CONDITION_OPERATORS, evaluate_condition_block
from robotocore.services.iam.policy_engine import (
    _principal_matches,
    _statement_matches_action,
    _statement_matches_resource,
)

ALLOWED = "allowed"
EXPLICIT_DENY = "explicitDeny"
IMPLICIT_DENY = "implicitDeny"

# The SourcePolicyType AWS reports for identity policies.
IAM_POLICY_TYPE = "IAM Policy"
RESOURCE_POLICY_TYPE = "resource"

_VARIABLE_RE = re.compile(r"\$\{([^}]+)\}")
_PRINCIPAL_ARN_RE = re.compile(r"^arn:aws[a-z-]*:iam::(\d{12}):(user|group|role)(/.*)?/([^/]+)$")
_ACCOUNT_IN_ARN_RE = re.compile(r"^arn:aws[a-z-]*:[^:]*:[^:]*:(\d{12}):")


@dataclass(frozen=True)
class SourcePolicy:
    """One policy taking part in a simulation, labelled the way the response names it."""

    policy_id: str
    document: dict
    policy_type: str = IAM_POLICY_TYPE


@dataclass
class Decision:
    decision: str
    matched: list[SourcePolicy] = field(default_factory=list)
    missing_context: list[str] = field(default_factory=list)
    allowed_by_boundary: bool | None = None


class ContextValues(dict):
    """Request context keyed case-insensitively, as IAM condition keys are."""

    def __init__(self, entries: dict[str, Any] | None = None) -> None:
        super().__init__()
        for key, value in (entries or {}).items():
            self[key] = value

    def __setitem__(self, key: str, value: Any) -> None:
        super().__setitem__(key.lower(), value)

    def __getitem__(self, key: str) -> Any:
        return super().__getitem__(key.lower())

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and super().__contains__(key.lower())

    def get(self, key: str, default: Any = None) -> Any:  # type: ignore[override]
        return super().get(key.lower(), default)


class PrincipalNotFoundError(Exception):
    """The PolicySourceArn names a user, group or role that does not exist."""


class InvalidPrincipalArnError(Exception):
    """The PolicySourceArn is not an IAM user, group or role ARN."""


def load_policy_document(raw: str) -> dict:
    """Parse a policy document, accepting the URL-encoded form IAM sometimes stores."""
    for candidate in (raw, unquote(raw)):
        try:
            doc = json.loads(candidate)
        except ValueError:
            continue
        if isinstance(doc, dict):
            return doc
    raise ValueError("policy document is not a JSON object")


def _statements(document: dict) -> list[dict]:
    statements = document.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]
    return [s for s in statements if isinstance(s, dict)]


def _as_list(value: Any) -> list:
    return value if isinstance(value, list) else [value]


def _has_unsupported_operator(condition: dict) -> bool:
    for raw_operator in condition:
        operator = raw_operator
        for prefix in ("ForAllValues:", "ForAnyValue:"):
            if operator.startswith(prefix):
                operator = operator[len(prefix) :]
        operator = operator.removesuffix("IfExists")
        if operator != "Null" and operator not in CONDITION_OPERATORS:
            return True
    return False


def _condition_satisfied(statement: dict, context: ContextValues) -> bool:
    condition = statement.get("Condition")
    if not condition or not isinstance(condition, dict):
        return True
    if _has_unsupported_operator(condition):
        # An operator the engine cannot evaluate must never widen access: treat the
        # condition as unmet on an Allow and as met on a Deny.
        return statement.get("Effect") == "Deny"
    return evaluate_condition_block(condition, context)


def _note_missing(keys: Iterable[str], context: ContextValues, missing: list[str]) -> None:
    for key in keys:
        if key not in context and key.lower() not in (m.lower() for m in missing):
            missing.append(key)


def _resource_variables(statement: dict) -> list[str]:
    patterns = statement.get("Resource", statement.get("NotResource", []))
    return [v for p in _as_list(patterns) for v in _VARIABLE_RE.findall(str(p))]


def _condition_keys(statement: dict) -> list[str]:
    condition = statement.get("Condition")
    if not isinstance(condition, dict):
        return []
    return [key for block in condition.values() if isinstance(block, dict) for key in block]


@dataclass
class _Matches:
    allows: list[SourcePolicy] = field(default_factory=list)
    denies: list[SourcePolicy] = field(default_factory=list)


def _match(
    policies: list[SourcePolicy],
    action: str,
    resource: str,
    context: ContextValues,
    missing: list[str],
    principal: str | None = None,
) -> _Matches:
    """Collect the statements that apply, one entry per statement, by effect."""
    matches = _Matches()
    for policy in policies:
        for statement in _statements(policy.document):
            effect = statement.get("Effect")
            if effect not in ("Allow", "Deny"):
                continue
            if principal is not None and not _principal_matches(statement, principal):
                continue
            if not _statement_matches_action(statement, action):
                continue
            _note_missing(_resource_variables(statement), context, missing)
            if not _statement_matches_resource(statement, resource, context):
                continue
            _note_missing(_condition_keys(statement), context, missing)
            if not _condition_satisfied(statement, context):
                continue
            (matches.allows if effect == "Allow" else matches.denies).append(policy)
    return matches


def _boundary_limits_resource_grant(caller_arn: str) -> bool:
    # Same-account resource-policy grants to an IAM user are not limited by the user's
    # permissions boundary; grants to a role or role session are.
    return ":role/" in caller_arn or ":assumed-role/" in caller_arn


def simulate(
    *,
    identity: list[SourcePolicy],
    action: str,
    resource: str,
    context: ContextValues,
    boundary: list[SourcePolicy] | None = None,
    resource_policy: SourcePolicy | None = None,
    caller_arn: str = "",
    cross_account: bool = False,
) -> Decision:
    """Evaluate one action against one resource the way the IAM policy simulator does.

    An explicit Deny in any policy wins. Otherwise the identity policies must allow
    the action and, when a permissions boundary is present, so must the boundary. A
    resource policy can grant access on its own within one account; across accounts
    both the identity side and the resource policy must allow.
    """
    missing: list[str] = []
    ident = _match(identity, action, resource, context, missing)
    bound = _match(boundary, action, resource, context, missing) if boundary is not None else None
    res = (
        _match([resource_policy], action, resource, context, missing, principal=caller_arn)
        if resource_policy is not None
        else None
    )

    allowed_by_boundary = None if bound is None else bool(bound.allows) and not bound.denies
    denies = ident.denies + (bound.denies if bound else []) + (res.denies if res else [])
    if denies:
        return Decision(EXPLICIT_DENY, denies, missing, allowed_by_boundary)

    boundary_ok = bound is None or bool(bound.allows)
    identity_ok = bool(ident.allows) and boundary_ok
    resource_ok = res is not None and bool(res.allows)
    if resource_ok and not cross_account and _boundary_limits_resource_grant(caller_arn):
        resource_ok = boundary_ok

    if cross_account:
        allowed = identity_ok and resource_ok
    else:
        allowed = identity_ok or resource_ok
    if not allowed:
        return Decision(IMPLICIT_DENY, [], missing, allowed_by_boundary)

    matched: list[SourcePolicy] = []
    if identity_ok:
        matched += ident.allows + (bound.allows if bound else [])
    if resource_ok and res is not None:
        matched += res.allows
    return Decision(ALLOWED, matched, missing, allowed_by_boundary)


def account_of(arn_or_account: str) -> str | None:
    """The 12-digit account id in an ARN, or the value itself if it is an account id."""
    if re.fullmatch(r"\d{12}", arn_or_account):
        return arn_or_account
    match = _ACCOUNT_IN_ARN_RE.match(arn_or_account)
    return match.group(1) if match else None


def _managed_document(policy: Any) -> dict:
    for version in policy.versions:
        if version.is_default:
            return load_policy_document(version.document)
    return {}


def _inline(policies: dict[str, str]) -> list[SourcePolicy]:
    return [SourcePolicy(name, load_policy_document(doc)) for name, doc in policies.items()]


def _managed(policies: dict[str, Any]) -> list[SourcePolicy]:
    return [SourcePolicy(p.name, _managed_document(p)) for p in policies.values()]


def collect_principal_policies(
    backend: Any,
    principal_arn: str,
    account_id: str,
    resolve_managed_policy: Callable[[str], Any],
) -> tuple[list[SourcePolicy], list[SourcePolicy] | None]:
    """Gather a user's, group's or role's identity policies and its permissions boundary.

    A user's policies include those of every group it belongs to. Returns
    ``(identity_policies, boundary_policies)``; the boundary is None when the
    principal has none set.
    """
    match = _PRINCIPAL_ARN_RE.match(principal_arn)
    if not match:
        raise InvalidPrincipalArnError(principal_arn)
    arn_account, kind, _path, name = match.groups()
    not_found = PrincipalNotFoundError(f"The {kind} with name {name} cannot be found.")
    if arn_account != account_id:
        raise not_found

    if kind == "group":
        group = backend.groups.get(name)
        if group is None:
            raise not_found
        return _inline(group.policies) + _managed(group.managed_policies), None

    if kind == "user":
        entity = backend.users.get(name)
        if entity is None:
            raise not_found
        identity = _inline(entity.policies) + _managed(entity.managed_policies)
        for group in backend.groups.values():
            if any(member.name == entity.name for member in group.users):
                identity += _inline(group.policies) + _managed(group.managed_policies)
    else:
        entity = next((r for r in backend.roles.values() if r.name == name), None)
        if entity is None:
            raise not_found
        identity = _inline(entity.policies) + _managed(entity.managed_policies)

    boundary_arn = getattr(entity, "permissions_boundary_arn", None)
    if not boundary_arn:
        return identity, None
    boundary_policy = resolve_managed_policy(boundary_arn)
    # A boundary that cannot be resolved allows nothing rather than everything.
    boundary = [] if boundary_policy is None else _managed({boundary_arn: boundary_policy})
    return identity, boundary
