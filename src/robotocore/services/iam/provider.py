"""Native IAM provider.

Intercepts operations that Moto doesn't implement:
- SimulateCustomPolicy / SimulatePrincipalPolicy: Evaluates policies (see simulator.py)
- PutUserPermissionsBoundary / DeleteUserPermissionsBoundary: Stores boundary
- ChangePassword: No-op (requires user session in real AWS)
"""

import json
import logging
import re
import uuid
from typing import Any
from urllib.parse import parse_qs
from xml.sax.saxutils import escape as xml_escape

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto
from robotocore.services.iam.simulator import (
    RESOURCE_POLICY_TYPE,
    ContextValues,
    Decision,
    InvalidPrincipalArnError,
    PrincipalNotFoundError,
    SourcePolicy,
    account_of,
    collect_principal_policies,
    load_policy_document,
    simulate,
)

logger = logging.getLogger(__name__)


async def handle_iam_request(request: Request, region: str, account_id: str) -> Response:
    """Handle IAM requests, intercepting unimplemented operations."""
    body = await request.body()
    params = parse_qs(body.decode("utf-8")) if body else {}
    for key, val in request.query_params.items():
        if key not in params:
            params[key] = [val]

    action = _get_param(params, "Action")
    handler = _ACTION_MAP.get(action)
    if handler:
        return handler(params, region, account_id)

    for key in _POLICY_ARN_PARAMS:
        arn = _get_param(params, key)
        if arn:
            _ensure_aws_managed_policy(arn, account_id)

    response = await forward_to_moto(request, "iam", account_id=account_id)

    if action == "SetSecurityTokenServicePreferences" and response.status_code == 200:
        _record_sts_token_version(_get_param(params, "GlobalEndpointTokenVersion"), account_id)

    # Post-process GetUser to inject PermissionsBoundary if set
    if action == "GetUser" and response.status_code == 200:
        response = _inject_user_permissions_boundary(
            response, _get_param(params, "UserName"), account_id
        )

    return response


# Request parameters that may name an AWS-managed policy (arn:<partition>:iam::aws:policy/...)
_POLICY_ARN_PARAMS = ("PolicyArn", "PermissionsBoundary")
_AWS_MANAGED_ARN_RE = re.compile(r"^arn:(aws[a-z-]*):iam::aws:policy(/.*/|/)([^/]+)$")
_aws_managed_index: dict[str, dict] | None = None
# AWS-managed policies newer than moto's bundled catalog. Only existence and attachability are
# modelled; the document is a placeholder, not AWS's published statement list.
_AWS_MANAGED_SUPPLEMENT: dict[str, dict] = {
    "BedrockAgentCoreRuntimeInstancesOperatorRolePolicy": {
        "DefaultVersionId": "v1",
        "Path": "/",
        "Document": {"Version": "2012-10-17", "Statement": []},
        "CreateDate": "2025-07-16T00:00:00+00:00",
        "UpdateDate": "2025-07-16T00:00:00+00:00",
    },
}


def _aws_managed_policy_data(path: str, name: str) -> dict | None:
    """Moto's bundled copy of the AWS-managed policy catalog, indexed by path+name."""
    global _aws_managed_index
    if _aws_managed_index is None:
        from moto.iam.aws_managed_policies import aws_managed_policies_data

        parsed = json.loads(aws_managed_policies_data)
        for extra_name, extra in _AWS_MANAGED_SUPPLEMENT.items():
            parsed.setdefault(extra_name, extra)
        _aws_managed_index = {
            f"{d.get('Path', '/')}{n}": {"name": n, **d} for n, d in parsed.items()
        }
    return _aws_managed_index.get(f"{path}{name}")


def _ensure_aws_managed_policy(arn: str, account_id: str) -> None:
    """Materialize one AWS-managed policy in the caller's account on first reference.

    AWS-managed policies exist in every account. Moto only loads them when
    MOTO_IAM_LOAD_MANAGED_POLICIES is set, which puts ~1,300 policies into every account's
    ListPolicies. Loading just the policies a caller actually names (AttachRolePolicy,
    GetPolicy, permissions boundaries, ...) keeps ListPolicies(Scope=Local) unchanged while
    making `arn:aws:iam::aws:policy/ReadOnlyAccess` attachable, as it is in AWS.
    """
    match = _AWS_MANAGED_ARN_RE.match(arn)
    if not match:
        return
    partition, path, name = match.groups()
    data = _aws_managed_policy_data(path, name)
    if data is None:
        return
    from moto.iam.models import AWSManagedPolicy, iam_backends

    backend = iam_backends[account_id][partition]
    if arn in backend.managed_policies:
        return
    policy = AWSManagedPolicy.from_data(name, account_id, backend.region_name, data)
    if policy.arn == arn:
        backend.managed_policies[arn] = policy
    else:
        logger.debug("aws-managed policy arn mismatch: wanted %s built %s", arn, policy.arn)


def _record_sts_token_version(version: str, account_id: str) -> None:
    """Reflect SetSecurityTokenServicePreferences in GetAccountSummary, as AWS does.

    Moto stores the preference but its AccountSummary hardcodes GlobalEndpointTokenVersion=1.
    """
    from moto.iam.models import iam_backends

    summary = iam_backends[account_id]["aws"].account_summary
    summary._global_endpoint_token_version = 2 if version == "v2Token" else 1


def _get_param(params: dict, key: str) -> str:
    vals = params.get(key, [])
    return vals[0] if vals else ""


def _inject_user_permissions_boundary(
    response: Response, user_name: str, account_id: str
) -> Response:
    """Inject PermissionsBoundary into GetUser response if set on the user."""
    try:
        from moto.backends import get_backend  # noqa: I001
        from moto.iam.exceptions import NoSuchEntity

        backend = get_backend("iam")[account_id]["global"]
        user = backend.get_user(user_name)
        if user.permissions_boundary_arn:
            body = response.body.decode("utf-8")
            boundary_xml = (
                f"<PermissionsBoundary>"
                f"<PermissionsBoundaryType>PermissionsBoundaryPolicy"
                f"</PermissionsBoundaryType>"
                f"<PermissionsBoundaryArn>{user.permissions_boundary_arn}"
                f"</PermissionsBoundaryArn>"
                f"</PermissionsBoundary>"
            )
            body = body.replace("</User>", f"{boundary_xml}</User>")
            headers = {k: v for k, v in response.headers.items() if k.lower() != "content-length"}
            return Response(
                content=body,
                status_code=response.status_code,
                headers=headers,
                media_type="text/xml",
            )
    except NoSuchEntity as exc:
        # User not found in Moto backend — this is expected when GetUser
        # returns a default_user from access key lookup. Leave response as-is.
        logger.debug("_inject_user_permissions_boundary: get_user failed (non-fatal): %s", exc)
    except Exception:  # noqa: BLE001
        logger.warning("Failed to inject PermissionsBoundary into GetUser response", exc_info=True)
    return response


def _iam_error_response(code: str, message: str, status_code: int = 400) -> Response:
    """Return a properly formatted IAM XML error response."""
    xml = (
        f'<ErrorResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">'
        f"<Error>"
        f"<Type>Sender</Type>"
        f"<Code>{code}</Code>"
        f"<Message>{message}</Message>"
        f"</Error>"
        f"<RequestId>{uuid.uuid4()}</RequestId>"
        f"</ErrorResponse>"
    )
    return Response(content=xml, status_code=status_code, media_type="text/xml")


def _get_list_param(params: dict, prefix: str) -> list[str]:
    result = []
    i = 1
    while True:
        val = _get_param(params, f"{prefix}.member.{i}")
        if not val:
            break
        result.append(val)
        i += 1
    return result


def _simulation_policy_inputs(params: dict, prefix: str) -> list[SourcePolicy]:
    """Parse PolicyInputList-style members; raises ValueError naming a malformed one."""
    policies = []
    for i, raw in enumerate(_get_list_param(params, prefix), start=1):
        policy_id = f"{prefix}.{i}"
        try:
            policies.append(SourcePolicy(policy_id, load_policy_document(raw)))
        except ValueError as exc:
            raise ValueError(f"Policy {policy_id} is not a valid JSON policy document.") from exc
    return policies


def _simulation_context(params: dict) -> ContextValues:
    """ContextEntries.member.N -> condition-key values (lists for the *List key types)."""
    context = ContextValues()
    i = 1
    while name := _get_param(params, f"ContextEntries.member.{i}.ContextKeyName"):
        values = _get_list_param(params, f"ContextEntries.member.{i}.ContextKeyValues")
        key_type = _get_param(params, f"ContextEntries.member.{i}.ContextKeyType")
        if values:
            context[name] = values if key_type.endswith("List") else values[0]
        i += 1
    return context


def _render_matched(matched: list[SourcePolicy]) -> str:
    members = "".join(
        f"<member><SourcePolicyId>{xml_escape(p.policy_id)}</SourcePolicyId>"
        f"<SourcePolicyType>{xml_escape(p.policy_type)}</SourcePolicyType></member>"
        for p in matched
    )
    return f"<MatchedStatements>{members}</MatchedStatements>"


def _render_decision_body(decision: Decision) -> str:
    missing = "".join(f"<member>{xml_escape(k)}</member>" for k in decision.missing_context)
    body = (
        f"{_render_matched(decision.matched)}<MissingContextValues>{missing}</MissingContextValues>"
    )
    if decision.allowed_by_boundary is not None:
        allowed = "true" if decision.allowed_by_boundary else "false"
        body += (
            "<PermissionsBoundaryDecisionDetail>"
            f"<AllowedByPermissionsBoundary>{allowed}</AllowedByPermissionsBoundary>"
            "</PermissionsBoundaryDecisionDetail>"
        )
    return body


def _render_evaluation_result(
    action: str, resource: str, decision: Decision, *, resource_specific: bool
) -> str:
    xml = (
        f"<member><EvalActionName>{xml_escape(action)}</EvalActionName>"
        f"<EvalResourceName>{xml_escape(resource)}</EvalResourceName>"
        f"<EvalDecision>{decision.decision}</EvalDecision>"
        f"{_render_decision_body(decision)}"
    )
    if resource_specific:
        xml += (
            "<ResourceSpecificResults><member>"
            f"<EvalResourceName>{xml_escape(resource)}</EvalResourceName>"
            f"<EvalResourceDecision>{decision.decision}</EvalResourceDecision>"
            f"{_render_decision_body(decision)}"
            "</member></ResourceSpecificResults>"
        )
    return xml + "</member>"


def _run_simulation(
    operation: str,
    params: dict,
    account_id: str,
    identity: list[SourcePolicy],
    boundary: list[SourcePolicy] | None,
    default_caller_arn: str = "",
) -> Response:
    try:
        identity = identity + _simulation_policy_inputs(params, "PolicyInputList")
        boundary_inputs = _simulation_policy_inputs(params, "PermissionsBoundaryPolicyInputList")
        raw_resource_policy = _get_param(params, "ResourcePolicy")
        resource_policy = (
            SourcePolicy(
                "ResourcePolicy",
                load_policy_document(raw_resource_policy),
                RESOURCE_POLICY_TYPE,
            )
            if raw_resource_policy
            else None
        )
    except ValueError as exc:
        return _iam_error_response("InvalidInput", xml_escape(str(exc)))
    if boundary_inputs:
        # Supplied boundary policies stand in for any boundary attached to the principal.
        boundary = boundary_inputs

    caller_arn = _get_param(params, "CallerArn") or default_caller_arn
    owner_account = account_of(_get_param(params, "ResourceOwner"))
    caller_account = account_of(caller_arn) or account_id
    cross_account = owner_account is not None and owner_account != caller_account

    context = _simulation_context(params)
    resource_arns = _get_list_param(params, "ResourceArns")
    results = [
        _render_evaluation_result(
            action,
            resource,
            simulate(
                identity=identity,
                boundary=boundary,
                resource_policy=resource_policy,
                action=action,
                resource=resource,
                context=context,
                caller_arn=caller_arn,
                cross_account=cross_account,
            ),
            resource_specific=bool(resource_arns),
        )
        for action in _get_list_param(params, "ActionNames")
        for resource in resource_arns or ["*"]
    ]

    marker = _get_param(params, "Marker")
    max_items = _get_param(params, "MaxItems")
    try:
        start = int(marker) if marker else 0
        limit = int(max_items) if max_items else max(len(results), 1)
    except ValueError:
        start, limit = -1, 0
    if start < 0 or limit < 1:
        return _iam_error_response("InvalidInput", "Invalid Marker or MaxItems.")
    page = results[start : start + limit]
    truncated = start + limit < len(results)
    marker_xml = f"<Marker>{start + limit}</Marker>" if truncated else ""

    xml = (
        f'<{operation}Response xmlns="https://iam.amazonaws.com/doc/2010-05-08/">'
        f"<{operation}Result>"
        f"<EvaluationResults>{''.join(page)}</EvaluationResults>"
        f"<IsTruncated>{'true' if truncated else 'false'}</IsTruncated>{marker_xml}"
        f"</{operation}Result>"
        f"<ResponseMetadata><RequestId>{uuid.uuid4()}</RequestId></ResponseMetadata>"
        f"</{operation}Response>"
    )
    return Response(content=xml, status_code=200, media_type="text/xml")


def _simulate_custom_policy(params: dict, region: str, account_id: str) -> Response:
    return _run_simulation("SimulateCustomPolicy", params, account_id, [], None)


def _resolve_managed_policy(arn: str, account_id: str) -> Any:
    from moto.backends import get_backend

    _ensure_aws_managed_policy(arn, account_id)
    return get_backend("iam")[account_id]["global"].managed_policies.get(arn)


def _simulate_principal_policy(params: dict, region: str, account_id: str) -> Response:
    from moto.backends import get_backend

    source_arn = _get_param(params, "PolicySourceArn")
    backend = get_backend("iam")[account_id]["global"]
    try:
        identity, boundary = collect_principal_policies(
            backend,
            source_arn,
            account_id,
            lambda arn: _resolve_managed_policy(arn, account_id),
        )
    except InvalidPrincipalArnError:
        return _iam_error_response(
            "InvalidInput", f"Invalid ARN {xml_escape(source_arn)}: expected a user, group or role."
        )
    except PrincipalNotFoundError as exc:
        return _iam_error_response("NoSuchEntity", xml_escape(str(exc)), 404)
    except ValueError as exc:
        logger.debug("SimulatePrincipalPolicy: unreadable stored policy: %s", exc)
        return _iam_error_response(
            "InvalidInput", "A policy attached to the principal is malformed."
        )
    default_caller = source_arn if ":user/" in source_arn else ""
    return _run_simulation(
        "SimulatePrincipalPolicy", params, account_id, identity, boundary, default_caller
    )


def _put_user_permissions_boundary(params: dict, region: str, account_id: str) -> Response:
    from moto.backends import get_backend  # noqa: I001
    from moto.iam.exceptions import NoSuchEntity

    user_name = _get_param(params, "UserName")
    boundary_arn = _get_param(params, "PermissionsBoundary")

    backend = get_backend("iam")[account_id]["global"]
    try:
        user = backend.get_user(user_name)
    except NoSuchEntity:
        return _iam_error_response(
            "NoSuchEntity",
            f"The user with name {user_name} cannot be found.",
            404,
        )
    user.permissions_boundary_arn = boundary_arn

    xml = f"""<PutUserPermissionsBoundaryResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ResponseMetadata>
    <RequestId>{uuid.uuid4()}</RequestId>
  </ResponseMetadata>
</PutUserPermissionsBoundaryResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _delete_user_permissions_boundary(params: dict, region: str, account_id: str) -> Response:
    from moto.backends import get_backend  # noqa: I001
    from moto.iam.exceptions import NoSuchEntity

    user_name = _get_param(params, "UserName")

    backend = get_backend("iam")[account_id]["global"]
    try:
        user = backend.get_user(user_name)
    except NoSuchEntity:
        return _iam_error_response(
            "NoSuchEntity",
            f"The user with name {user_name} cannot be found.",
            404,
        )
    user.permissions_boundary_arn = None

    xml = f"""<DeleteUserPermissionsBoundaryResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ResponseMetadata>
    <RequestId>{uuid.uuid4()}</RequestId>
  </ResponseMetadata>
</DeleteUserPermissionsBoundaryResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _change_password(params: dict, region: str, account_id: str) -> Response:
    xml = f"""<ChangePasswordResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ResponseMetadata>
    <RequestId>{uuid.uuid4()}</RequestId>
  </ResponseMetadata>
</ChangePasswordResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


_ACTION_MAP = {
    "SimulateCustomPolicy": _simulate_custom_policy,
    "SimulatePrincipalPolicy": _simulate_principal_policy,
    "PutUserPermissionsBoundary": _put_user_permissions_boundary,
    "DeleteUserPermissionsBoundary": _delete_user_permissions_boundary,
    "ChangePassword": _change_password,
}
