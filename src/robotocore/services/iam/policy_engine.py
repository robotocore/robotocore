"""IAM policy evaluation engine.

Implements the full IAM policy evaluation logic:
- Explicit deny > explicit allow > implicit deny
- Action and resource wildcard matching
- NotAction / NotResource support
- Policy variable substitution
- Permission boundaries
- Resource policy evaluation
"""

from __future__ import annotations

import re
from typing import Any

from robotocore.services.iam.conditions import evaluate_condition_block

# Result constants
ALLOW = "Allow"
DENY = "Deny"
IMPLICIT_DENY = "ImplicitDeny"

# Regex for policy variables like ${aws:username} or ${s3:prefix}
_VARIABLE_RE = re.compile(r"\$\{([^}]+)\}")


def _substitute_variables(value: str, context: dict[str, Any]) -> str:
    """Replace policy variables like ${aws:username} with context values."""

    def _replace(match: re.Match) -> str:
        var_name = match.group(1)
        return str(context.get(var_name, match.group(0)))

    return _VARIABLE_RE.sub(_replace, value)


def _iam_match(value: str, pattern: str) -> bool:
    """Match using IAM wildcard rules (only * is supported, not ? or [])."""
    regex_parts = []
    for part in re.split(r"(\*)", pattern):
        if part == "*":
            regex_parts.append(".*")
        else:
            regex_parts.append(re.escape(part))
    regex = "^" + "".join(regex_parts) + "$"
    return bool(re.match(regex, value, re.IGNORECASE))


def _action_matches(action: str, pattern: str) -> bool:
    """Check if an action matches a pattern (case-insensitive, only * wildcard)."""
    return _iam_match(action, pattern)


def _resource_matches(resource: str, pattern: str, context: dict[str, Any]) -> bool:
    """Check if a resource ARN matches a pattern (only * wildcard, with variable substitution)."""
    resolved_pattern = _substitute_variables(pattern, context)
    return _iam_match(resource, resolved_pattern)


def _statement_matches_action(statement: dict, action: str) -> bool:
    """Check if a statement's Action/NotAction matches the given action."""
    if "Action" in statement:
        actions = statement["Action"]
        if isinstance(actions, str):
            actions = [actions]
        return any(_action_matches(action, a) for a in actions)
    elif "NotAction" in statement:
        not_actions = statement["NotAction"]
        if isinstance(not_actions, str):
            not_actions = [not_actions]
        return not any(_action_matches(action, a) for a in not_actions)
    return False


def _statement_matches_resource(statement: dict, resource: str, context: dict[str, Any]) -> bool:
    """Check if a statement's Resource/NotResource matches the given resource."""
    if "Resource" in statement:
        resources = statement["Resource"]
        if isinstance(resources, str):
            resources = [resources]
        return any(_resource_matches(resource, r, context) for r in resources)
    elif "NotResource" in statement:
        not_resources = statement["NotResource"]
        if isinstance(not_resources, str):
            not_resources = [not_resources]
        return not any(_resource_matches(resource, r, context) for r in not_resources)
    # No Resource/NotResource means matches everything (e.g., identity-based without Resource)
    return True


def _evaluate_statement(
    statement: dict,
    action: str,
    resource: str,
    context: dict[str, Any],
) -> str | None:
    """Evaluate a single policy statement.

    Returns "Allow", "Deny", or None if the statement does not apply.
    """
    effect = statement.get("Effect", "")
    if effect not in (ALLOW, DENY):
        return None

    if not _statement_matches_action(statement, action):
        return None

    if not _statement_matches_resource(statement, resource, context):
        return None

    # Check conditions
    condition = statement.get("Condition")
    if condition and not evaluate_condition_block(condition, context):
        return None

    return effect


def evaluate_policy(
    policies: list[dict],
    action: str,
    resource: str,
    context: dict[str, Any] | None = None,
) -> str:
    """Evaluate a list of IAM policies against an action and resource.

    Returns "Allow", "Deny", or "ImplicitDeny".

    Logic: explicit Deny beats explicit Allow beats implicit deny.
    """
    if context is None:
        context = {}

    has_allow = False

    for policy in policies:
        statements = policy.get("Statement", [])
        if isinstance(statements, dict):
            statements = [statements]

        for statement in statements:
            result = _evaluate_statement(statement, action, resource, context)
            if result == DENY:
                return DENY
            if result == ALLOW:
                has_allow = True

    return ALLOW if has_allow else IMPLICIT_DENY


def evaluate_with_permission_boundary(
    identity_policies: list[dict],
    permission_boundary: dict | None,
    action: str,
    resource: str,
    context: dict[str, Any] | None = None,
) -> str:
    """Evaluate policies with a permission boundary.

    The effective permission is the intersection of identity policies
    and the permission boundary. Both must allow the action.
    """
    if context is None:
        context = {}

    identity_result = evaluate_policy(identity_policies, action, resource, context)
    if identity_result == DENY:
        return DENY

    if permission_boundary is not None:
        boundary_result = evaluate_policy([permission_boundary], action, resource, context)
        if boundary_result == DENY:
            return DENY
        if boundary_result != ALLOW:
            return IMPLICIT_DENY

    return identity_result


def _principal_matches(statement: dict, principal: str) -> bool:
    """Check if a statement's Principal or NotPrincipal matches the given principal."""
    # Handle NotPrincipal: matches everyone EXCEPT the listed principals
    if "NotPrincipal" in statement:
        not_principal = statement["NotPrincipal"]
        # Check if the principal IS in the NotPrincipal list
        if not_principal == "*":
            return False  # NotPrincipal: * means nobody matches
        if isinstance(not_principal, str):
            if _iam_match(principal, not_principal):
                return False  # Principal is excluded
            return True  # Principal is not excluded, so statement applies
        if isinstance(not_principal, dict):
            for _key, values in not_principal.items():
                if isinstance(values, str):
                    values = [values]
                for v in values:
                    if v == "*" or _iam_match(principal, v):
                        return False  # Principal is excluded
            return True  # Principal is not excluded
        return True

    stmt_principal = statement.get("Principal")
    if stmt_principal is None:
        return True

    if stmt_principal == "*":
        return True

    if isinstance(stmt_principal, str):
        return _iam_match(principal, stmt_principal)

    if isinstance(stmt_principal, dict):
        # Principal can be {"AWS": "arn:..."} or {"AWS": ["arn:...", ...]}
        for _key, values in stmt_principal.items():
            if isinstance(values, str):
                values = [values]
            for v in values:
                if v == "*" or _iam_match(principal, v):
                    return True
    return False


def evaluate_resource_policy(
    policy: dict,
    principal: str,
    action: str,
    resource: str,
    context: dict[str, Any] | None = None,
) -> str:
    """Evaluate a resource-based policy.

    Resource policies can grant cross-account access. They include
    a Principal element that must match.

    Returns "Allow", "Deny", or "ImplicitDeny".
    """
    if context is None:
        context = {}

    has_allow = False

    statements = policy.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]

    for statement in statements:
        if not _principal_matches(statement, principal):
            continue

        result = _evaluate_statement(statement, action, resource, context)
        if result == DENY:
            return DENY
        if result == ALLOW:
            has_allow = True

    return ALLOW if has_allow else IMPLICIT_DENY
