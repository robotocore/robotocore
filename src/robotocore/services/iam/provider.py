"""Native IAM provider.

Intercepts operations that Moto doesn't implement:
- SimulateCustomPolicy / SimulatePrincipalPolicy: Returns simulated results
- PutUserPermissionsBoundary / DeleteUserPermissionsBoundary: Stores boundary
- ChangePassword: No-op (requires user session in real AWS)
"""

import json
import logging
import re
import uuid
from urllib.parse import parse_qs

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

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


def _simulate_custom_policy(params: dict, region: str, account_id: str) -> Response:
    action_names = _get_list_param(params, "ActionNames")

    results_xml = ""
    for action_name in action_names:
        results_xml += f"""      <member>
        <EvalActionName>{action_name}</EvalActionName>
        <EvalDecision>allowed</EvalDecision>
        <MatchedStatements>
          <member>
            <SourcePolicyId>PolicyInputList.1</SourcePolicyId>
            <SourcePolicyType>IAM Policy</SourcePolicyType>
          </member>
        </MatchedStatements>
        <MissingContextValues/>
        <EvalResourceName>*</EvalResourceName>
      </member>
"""

    xml = f"""<SimulateCustomPolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <SimulateCustomPolicyResult>
    <EvaluationResults>
{results_xml}    </EvaluationResults>
    <IsTruncated>false</IsTruncated>
  </SimulateCustomPolicyResult>
  <ResponseMetadata>
    <RequestId>{uuid.uuid4()}</RequestId>
  </ResponseMetadata>
</SimulateCustomPolicyResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _simulate_principal_policy(params: dict, region: str, account_id: str) -> Response:
    action_names = _get_list_param(params, "ActionNames")

    results_xml = ""
    for action_name in action_names:
        results_xml += f"""      <member>
        <EvalActionName>{action_name}</EvalActionName>
        <EvalDecision>allowed</EvalDecision>
        <MatchedStatements/>
        <MissingContextValues/>
        <EvalResourceName>*</EvalResourceName>
      </member>
"""

    xml = f"""<SimulatePrincipalPolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <SimulatePrincipalPolicyResult>
    <EvaluationResults>
{results_xml}    </EvaluationResults>
    <IsTruncated>false</IsTruncated>
  </SimulatePrincipalPolicyResult>
  <ResponseMetadata>
    <RequestId>{uuid.uuid4()}</RequestId>
  </ResponseMetadata>
</SimulatePrincipalPolicyResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


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
