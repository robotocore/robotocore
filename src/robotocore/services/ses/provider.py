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
    from moto.backends import get_backend  # noqa: I001

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


def _create_configuration_set_tracking_options(params: dict, region: str, account_id: str) -> str:
    """CreateConfigurationSetTrackingOptions — set custom redirect domain."""
    backend = _get_ses_backend(account_id, region)
    cs_name = params.get("ConfigurationSetName", "")
    if cs_name not in backend.config_sets:
        raise SesError(
            "ConfigurationSetDoesNotExist", f"Configuration set <{cs_name}> does not exist."
        )
    custom_domain = params.get("TrackingOptions.CustomRedirectDomain", "")
    backend.config_sets[cs_name].tracking_options = {"CustomRedirectDomain": custom_domain}
    return ""


def _update_configuration_set_tracking_options(params: dict, region: str, account_id: str) -> str:
    """UpdateConfigurationSetTrackingOptions — update custom redirect domain."""
    backend = _get_ses_backend(account_id, region)
    cs_name = params.get("ConfigurationSetName", "")
    if cs_name not in backend.config_sets:
        raise SesError(
            "ConfigurationSetDoesNotExist", f"Configuration set <{cs_name}> does not exist."
        )
    custom_domain = params.get("TrackingOptions.CustomRedirectDomain", "")
    backend.config_sets[cs_name].tracking_options = {"CustomRedirectDomain": custom_domain}
    return ""


def _delete_configuration_set_tracking_options(params: dict, region: str, account_id: str) -> str:
    """DeleteConfigurationSetTrackingOptions — remove tracking options."""
    backend = _get_ses_backend(account_id, region)
    cs_name = params.get("ConfigurationSetName", "")
    if cs_name not in backend.config_sets:
        raise SesError(
            "ConfigurationSetDoesNotExist", f"Configuration set <{cs_name}> does not exist."
        )
    backend.config_sets[cs_name].tracking_options = {}
    return ""


def _put_configuration_set_delivery_options(params: dict, region: str, account_id: str) -> str:
    """PutConfigurationSetDeliveryOptions — set TLS policy."""
    backend = _get_ses_backend(account_id, region)
    cs_name = params.get("ConfigurationSetName", "")
    if cs_name not in backend.config_sets:
        raise SesError(
            "ConfigurationSetDoesNotExist", f"Configuration set <{cs_name}> does not exist."
        )
    tls_policy = params.get("DeliveryOptions.TlsPolicy", "Optional")
    backend.config_sets[cs_name].delivery_options = {"TlsPolicy": tls_policy}
    return ""


def _update_configuration_set_sending_enabled(params: dict, region: str, account_id: str) -> str:
    """UpdateConfigurationSetSendingEnabled — enable/disable sending for a config set."""
    backend = _get_ses_backend(account_id, region)
    cs_name = params.get("ConfigurationSetName", "")
    if cs_name not in backend.config_sets:
        raise SesError(
            "ConfigurationSetDoesNotExist", f"Configuration set <{cs_name}> does not exist."
        )
    enabled_str = params.get("Enabled", "true")
    backend.config_sets[cs_name].enabled = {"SendingEnabled": enabled_str.lower() == "true"}
    return ""


def _delete_configuration_set_event_destination(params: dict, region: str, account_id: str) -> str:
    """DeleteConfigurationSetEventDestination — remove an event destination."""
    backend = _get_ses_backend(account_id, region)
    cs_name = params.get("ConfigurationSetName", "")
    dest_name = params.get("EventDestinationName", "")
    if cs_name not in backend.config_sets:
        raise SesError(
            "ConfigurationSetDoesNotExist", f"Configuration set <{cs_name}> does not exist."
        )
    # Remove from config_set_event_destination
    if cs_name in backend.config_set_event_destination:
        existing = backend.config_set_event_destination[cs_name]
        if isinstance(existing, list):
            backend.config_set_event_destination[cs_name] = [
                d for d in existing if d.get("Name") != dest_name
            ]
        elif isinstance(existing, dict) and existing.get("Name") == dest_name:
            del backend.config_set_event_destination[cs_name]
    # Remove from event_destinations index
    backend.event_destinations.pop(dest_name, None)
    return ""


def _update_configuration_set_event_destination(params: dict, region: str, account_id: str) -> str:
    """UpdateConfigurationSetEventDestination — update an event destination."""
    backend = _get_ses_backend(account_id, region)
    cs_name = params.get("ConfigurationSetName", "")
    dest_name = params.get("EventDestination.Name", "")
    if cs_name not in backend.config_sets:
        raise SesError(
            "ConfigurationSetDoesNotExist", f"Configuration set <{cs_name}> does not exist."
        )
    # Build the updated destination dict
    enabled_str = params.get("EventDestination.Enabled", "true")
    event_dest: dict = {
        "Name": dest_name,
        "Enabled": enabled_str.lower() == "true",
        "MatchingEventTypes": [],
    }
    # Extract matching event types (EventDestination.MatchingEventTypes.member.N)
    for key, value in params.items():
        if key.startswith("EventDestination.MatchingEventTypes.member."):
            event_dest["MatchingEventTypes"].append(value)

    # Update the stored destination
    if cs_name in backend.config_set_event_destination:
        existing = backend.config_set_event_destination[cs_name]
        if isinstance(existing, dict) and existing.get("Name") == dest_name:
            backend.config_set_event_destination[cs_name] = event_dest
        elif isinstance(existing, list):
            updated = [event_dest if d.get("Name") == dest_name else d for d in existing]
            backend.config_set_event_destination[cs_name] = updated
    return ""


def _put_identity_policy(params: dict, region: str, account_id: str) -> str:
    """PutIdentityPolicy — attach a sending authorization policy to an identity."""
    backend = _get_ses_backend(account_id, region)
    identity = params.get("Identity", "")
    policy_name = params.get("PolicyName", "")
    policy = params.get("Policy", "")
    if not hasattr(backend, "identity_policies"):
        backend.identity_policies = {}
    if identity not in backend.identity_policies:
        backend.identity_policies[identity] = {}
    backend.identity_policies[identity][policy_name] = policy
    return ""


def _get_identity_policies(params: dict, region: str, account_id: str) -> str:
    """GetIdentityPolicies — retrieve sending authorization policies."""
    backend = _get_ses_backend(account_id, region)
    identity = params.get("Identity", "")
    # Collect requested policy names (PolicyNames.member.N)
    requested = []
    for key, value in params.items():
        if key.startswith("PolicyNames.member."):
            requested.append(value)

    identity_policies = getattr(backend, "identity_policies", {})
    policies = identity_policies.get(identity, {})

    # Build XML for each policy
    policy_items = ""
    for name in requested:
        if name in policies:
            policy_items += (
                f"<entry><key>{_escape_xml(name)}</key>"
                f"<value>{_escape_xml(policies[name])}</value></entry>"
            )
    return f"<Policies>{policy_items}</Policies>"


def _list_identity_policies(params: dict, region: str, account_id: str) -> str:
    """ListIdentityPolicies — list the names of sending authorization policies."""
    backend = _get_ses_backend(account_id, region)
    identity = params.get("Identity", "")
    identity_policies = getattr(backend, "identity_policies", {})
    policy_names = list(identity_policies.get(identity, {}).keys())
    members = "".join(f"<member>{_escape_xml(n)}</member>" for n in policy_names)
    return f"<PolicyNames>{members}</PolicyNames>"


def _delete_identity_policy(params: dict, region: str, account_id: str) -> str:
    """DeleteIdentityPolicy — remove a sending authorization policy."""
    backend = _get_ses_backend(account_id, region)
    identity = params.get("Identity", "")
    policy_name = params.get("PolicyName", "")
    identity_policies = getattr(backend, "identity_policies", {})
    if identity in identity_policies:
        identity_policies[identity].pop(policy_name, None)
    return ""


def _set_identity_headers_in_notifications_enabled(
    params: dict, region: str, account_id: str
) -> str:
    """SetIdentityHeadersInNotificationsEnabled — no-op acknowledgement."""
    # This controls whether email headers are included in SNS notifications.
    # In our emulator we acknowledge the request but don't need to persist it.
    return ""


def _set_receipt_rule_position(params: dict, region: str, account_id: str) -> str:
    """SetReceiptRulePosition — reorder a rule within a rule set."""
    backend = _get_ses_backend(account_id, region)
    rule_set_name = params.get("RuleSetName", "")
    rule_name = params.get("RuleName", "")
    after = params.get("After", "")

    rule_set = backend.receipt_rule_set.get(rule_set_name)
    if rule_set is None:
        raise SesError("RuleSetDoesNotExist", f"Rule set does not exist: {rule_set_name}")

    rules = rule_set.rules
    # Find and remove the target rule
    target = next((r for r in rules if r.get("Name") == rule_name), None)
    if target is None:
        raise SesError("RuleDoesNotExist", f"Rule does not exist: {rule_name}")

    rules_without = [r for r in rules if r.get("Name") != rule_name]

    if not after:
        # Insert at beginning
        rule_set.rules = [target] + rules_without
    else:
        # Insert after the named rule
        after_rule = next((r for r in rules_without if r.get("Name") == after), None)
        if after_rule is None:
            raise SesError("RuleDoesNotExist", f"Rule does not exist: {after}")
        insert_idx = rules_without.index(after_rule) + 1
        rules_without.insert(insert_idx, target)
        rule_set.rules = rules_without

    return ""


def _reorder_receipt_rule_set(params: dict, region: str, account_id: str) -> str:
    """ReorderReceiptRuleSet — reorder all rules in a rule set."""
    backend = _get_ses_backend(account_id, region)
    rule_set_name = params.get("RuleSetName", "")

    rule_set = backend.receipt_rule_set.get(rule_set_name)
    if rule_set is None:
        raise SesError("RuleSetDoesNotExist", f"Rule set does not exist: {rule_set_name}")

    # Extract ordered rule names (RuleNames.member.N)
    ordered_names = []
    for key, value in params.items():
        if key.startswith("RuleNames.member."):
            ordered_names.append((int(key.split(".")[-1]), value))
    ordered_names.sort(key=lambda x: x[0])
    rule_names = [name for _, name in ordered_names]

    # Reorder the rules
    rule_map = {r.get("Name"): r for r in rule_set.rules}
    reordered = []
    for name in rule_names:
        if name in rule_map:
            reordered.append(rule_map[name])
    # Append any rules not in the list at the end
    for rule in rule_set.rules:
        if rule.get("Name") not in rule_names:
            reordered.append(rule)
    rule_set.rules = reordered
    return ""


def _delete_verified_email_address(params: dict, region: str, account_id: str) -> str:
    """DeleteVerifiedEmailAddress — legacy alias for DeleteIdentity."""
    backend = _get_ses_backend(account_id, region)
    email = params.get("EmailAddress", "")
    backend.email_identities.pop(email, None)
    # Also remove from domains if present (shouldn't be, but defensive)
    if hasattr(backend, "domains"):
        backend.domains.discard(email)
    return ""


def _send_bounce(params: dict, region: str, account_id: str) -> str:
    """SendBounce — simulate a bounce notification."""
    import uuid as _uuid

    # We don't need to actually process the bounce; just return a message ID
    message_id = _uuid.uuid4().hex
    return f"<MessageId>{message_id}</MessageId>"


_ACTION_MAP = {
    "ListIdentities": _list_identities,
    "GetAccountSendingEnabled": _get_account_sending_enabled,
    "DeleteReceiptRule": _delete_receipt_rule,
    "CreateReceiptRule": _create_receipt_rule,
    "SetIdentityNotificationTopic": _set_identity_notification_topic,
    "SetIdentityDkimEnabled": _set_identity_dkim_enabled,
    "CreateConfigurationSetTrackingOptions": _create_configuration_set_tracking_options,
    "UpdateConfigurationSetTrackingOptions": _update_configuration_set_tracking_options,
    "DeleteConfigurationSetTrackingOptions": _delete_configuration_set_tracking_options,
    "PutConfigurationSetDeliveryOptions": _put_configuration_set_delivery_options,
    "UpdateConfigurationSetSendingEnabled": _update_configuration_set_sending_enabled,
    "DeleteConfigurationSetEventDestination": _delete_configuration_set_event_destination,
    "UpdateConfigurationSetEventDestination": _update_configuration_set_event_destination,
    "PutIdentityPolicy": _put_identity_policy,
    "GetIdentityPolicies": _get_identity_policies,
    "ListIdentityPolicies": _list_identity_policies,
    "DeleteIdentityPolicy": _delete_identity_policy,
    "SetIdentityHeadersInNotificationsEnabled": _set_identity_headers_in_notifications_enabled,
    "SetReceiptRulePosition": _set_receipt_rule_position,
    "ReorderReceiptRuleSet": _reorder_receipt_rule_set,
    "DeleteVerifiedEmailAddress": _delete_verified_email_address,
    "SendBounce": _send_bounce,
}
