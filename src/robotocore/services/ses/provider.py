"""Native SES provider.

Intercepts operations that Moto doesn't support or handles incorrectly:
- ListIdentities with MaxItems (Moto ignores MaxItems)
- GetAccountSendingEnabled (not in Moto)
- DeleteReceiptRule (not in Moto, only DeleteReceiptRuleSet exists)
- CreateReceiptRule cleanup support

Delegates everything else to Moto via forward_to_moto.
Uses query protocol (Action parameter).
"""

import logging
from urllib.parse import parse_qs

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

logger = logging.getLogger(__name__)


def _get_ses_backend(account_id: str, region: str):
    """Get the Moto SES backend."""
    from moto.backends import get_backend

    return get_backend("ses")[account_id][region]


async def handle_ses_request(request: Request, region: str, account_id: str) -> Response:
    """Handle SES API requests (query protocol via Action parameter)."""
    body = await request.body()
    content_type = request.headers.get("content-type", "")

    if "x-www-form-urlencoded" in content_type:
        parsed = parse_qs(body.decode(), keep_blank_values=True)
    else:
        parsed = parse_qs(str(request.url.query), keep_blank_values=True)

    params = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
    action = params.get("Action", "")

    handler = _ACTION_MAP.get(action)
    if handler is not None:
        try:
            result = handler(params, region, account_id)
            return _xml_response(action, result)
        except SesError as e:
            return _error_response(e.code, e.message, e.status)
        except Exception as e:
            logger.exception("SES provider error for %s", action)
            return _error_response("InternalError", str(e), 500)

    # Fall back to Moto
    return await forward_to_moto(request, "ses", account_id=account_id)


class SesError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


# ---------------------------------------------------------------------------
# Operation handlers
# ---------------------------------------------------------------------------


def _list_identities(params: dict, region: str, account_id: str) -> str:
    """ListIdentities with MaxItems support."""
    backend = _get_ses_backend(account_id, region)
    identity_type = params.get("IdentityType")
    max_items = params.get("MaxItems")

    identities = backend.list_identities(identity_type)

    if max_items is not None:
        try:
            max_items = int(max_items)
            identities = identities[:max_items]
        except (ValueError, TypeError) as exc:
            logger.debug("_list_identities: int failed (non-fatal): %s", exc)

    # Build XML members
    members = "".join(f"<member>{_escape_xml(i)}</member>" for i in identities)
    return f"<Identities>{members}</Identities>"


def _get_account_sending_enabled(params: dict, region: str, account_id: str) -> str:
    """GetAccountSendingEnabled - not in Moto, return enabled=true."""
    return "<Enabled>true</Enabled>"


def _delete_receipt_rule(params: dict, region: str, account_id: str) -> str:
    """DeleteReceiptRule - not in Moto (only DeleteReceiptRuleSet exists).

    Removes a specific rule from a rule set.
    """
    backend = _get_ses_backend(account_id, region)
    rule_set_name = params.get("RuleSetName", "")
    rule_name = params.get("RuleName", "")

    if not rule_set_name or not rule_name:
        raise SesError(
            "ValidationError",
            "RuleSetName and RuleName are required",
        )

    rule_set = backend.receipt_rule_set.get(rule_set_name)
    if rule_set is None:
        raise SesError(
            "RuleSetDoesNotExist",
            f"Rule set does not exist: {rule_set_name}",
        )

    # Remove the rule from the rule set
    original_count = len(rule_set.rules)
    rule_set.rules = [r for r in rule_set.rules if r.get("Name") != rule_name]

    if len(rule_set.rules) == original_count:
        # Rule not found - AWS silently succeeds, so we do too
        pass

    return ""


def _create_receipt_rule(params: dict, region: str, account_id: str) -> str:
    """CreateReceiptRule - delegate to Moto backend directly.

    We intercept this to ensure proper parameter extraction from query params.
    """
    backend = _get_ses_backend(account_id, region)
    rule_set_name = params.get("RuleSetName", "")

    # Extract rule from query parameters
    rule: dict = {}
    rule["Name"] = params.get("Rule.Name", "")
    rule["Enabled"] = params.get("Rule.Enabled", "true").lower() == "true"
    rule["TlsPolicy"] = params.get("Rule.TlsPolicy", "Optional")
    rule["ScanEnabled"] = params.get("Rule.ScanEnabled", "false").lower() == "true"

    # Extract recipients
    recipients = []
    for key, value in params.items():
        if key.startswith("Rule.Recipients.member."):
            recipients.append(value if isinstance(value, str) else value[0])
    if recipients:
        rule["Recipients"] = recipients

    # Extract actions
    actions = []
    for key, value in params.items():
        if key.startswith("Rule.Actions.member."):
            # Simple action extraction
            parts = key.split(".")
            if len(parts) >= 4:
                actions.append(value if isinstance(value, str) else value[0])
    rule["Actions"] = actions

    after = params.get("After")

    try:
        backend.create_receipt_rule(rule_set_name, rule, after)
    except Exception as e:  # noqa: BLE001
        error_name = type(e).__name__
        raise SesError(error_name, str(e))

    return ""


def _set_identity_notification_topic(params: dict, region: str, account_id: str) -> str:
    """SetIdentityNotificationTopic - workaround for Moto KeyError when clearing topic."""
    backend = _get_ses_backend(account_id, region)
    identity = params.get("Identity", "")
    notification_type = params.get("NotificationType", "")
    sns_topic = params.get("SnsTopic")

    # Validate that the identity exists (email or domain)
    identity_exists = identity in backend.email_identities or identity in backend.domains
    if not identity_exists:
        raise SesError(
            "InvalidParameterValue",
            f"Identity '{identity}' does not exist.",
        )

    identity_sns_topics = backend.sns_topics.get(identity, {})
    if sns_topic is None or sns_topic == "":
        # Clear the topic — Moto raises KeyError if it doesn't exist
        identity_sns_topics.pop(notification_type, None)
    else:
        identity_sns_topics[notification_type] = sns_topic
    backend.sns_topics[identity] = identity_sns_topics

    return ""


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------


def _escape_xml(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _xml_response(action: str, body_content: str) -> Response:
    xml = (
        f'<{action}Response xmlns="http://ses.amazonaws.com/doc/2010-12-01/">'
        f"<{action}Result>{body_content}</{action}Result>"
        f"<ResponseMetadata>"
        f"<RequestId>00000000-0000-0000-0000-000000000000</RequestId>"
        f"</ResponseMetadata>"
        f"</{action}Response>"
    )
    return Response(content=xml, status_code=200, media_type="text/xml")


def _error_response(code: str, message: str, status: int) -> Response:
    xml = (
        f"<ErrorResponse>"
        f"<Error><Code>{_escape_xml(code)}</Code>"
        f"<Message>{_escape_xml(message)}</Message></Error>"
        f"<RequestId>00000000-0000-0000-0000-000000000000</RequestId>"
        f"</ErrorResponse>"
    )
    return Response(content=xml, status_code=status, media_type="text/xml")


# ---------------------------------------------------------------------------
# Action dispatch map
# ---------------------------------------------------------------------------


def _set_identity_dkim_enabled(params: dict, region: str, account_id: str) -> str:
    """SetIdentityDkimEnabled — acknowledge the request (no-op in mock)."""
    return ""


_ACTION_MAP = {
    "ListIdentities": _list_identities,
    "GetAccountSendingEnabled": _get_account_sending_enabled,
    "DeleteReceiptRule": _delete_receipt_rule,
    "CreateReceiptRule": _create_receipt_rule,
    "SetIdentityNotificationTopic": _set_identity_notification_topic,
    "SetIdentityDkimEnabled": _set_identity_dkim_enabled,
}
