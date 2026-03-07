"""Native AWS Config provider.

Intercepts operations that Moto doesn't support or handles incorrectly:
- DescribeConfigRules with nonexistent rule names (proper error)
- DescribeComplianceByConfigRule (stub)
- PutEvaluations (without requiring TestMode)
- DescribeConfigRuleEvaluationStatus (stub)
- PutConfigRule with InputParameters

Delegates everything else to Moto via forward_to_moto.
Uses JSON protocol (X-Amz-Target: StarlingDoveService.*).
"""

import json
import logging
import time

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

logger = logging.getLogger(__name__)

# In-memory stores for features Moto doesn't support
# {(account_id, region): {rule_name: [evaluations]}}
_evaluations: dict[tuple[str, str], dict[str, list[dict]]] = {}
# {(account_id, region): {rule_name: status_dict}}
_evaluation_statuses: dict[tuple[str, str], dict[str, dict]] = {}


def _get_config_backend(account_id: str, region: str):
    """Get the Moto config backend."""
    from moto.backends import get_backend

    return get_backend("config")[account_id][region]


async def handle_config_request(
    request: Request, region: str, account_id: str
) -> Response:
    """Handle AWS Config API requests (JSON protocol via X-Amz-Target)."""
    body = await request.body()
    target = request.headers.get("x-amz-target", "")

    # Extract operation: "StarlingDoveService.DescribeConfigRules" -> "DescribeConfigRules"
    operation = target.split(".")[-1] if "." in target else target

    params = json.loads(body) if body else {}

    handler = _ACTION_MAP.get(operation)
    if handler is not None:
        try:
            result = handler(params, region, account_id)
            return Response(
                content=json.dumps(result),
                status_code=200,
                media_type="application/x-amz-json-1.1",
            )
        except ConfigError as e:
            return _error_response(e.code, e.message, e.status)
        except Exception as e:
            logger.exception("Config provider error for %s", operation)
            return _error_response("InternalError", str(e), 500)

    # Fall back to Moto
    return await forward_to_moto(request, "config")


class ConfigError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


# ---------------------------------------------------------------------------
# Operation handlers
# ---------------------------------------------------------------------------


def _put_config_rule(params: dict, region: str, account_id: str) -> dict:
    """PutConfigRule - delegates to Moto but ensures InputParameters is preserved."""
    backend = _get_config_backend(account_id, region)
    config_rule = params.get("ConfigRule", {})
    tags = params.get("Tags", [])

    rule_name = config_rule.get("ConfigRuleName", "")

    # Store and strip InputParameters before forwarding to Moto
    # (Moto validates against known managed rule constraints, which is too strict)
    input_parameters = config_rule.pop("InputParameters", "")

    # Let Moto handle the core creation
    rule_arn = backend.put_config_rule(config_rule, tags)

    # Ensure InputParameters is stored on the rule object
    if input_parameters and rule_name in backend.config_rules:
        rule = backend.config_rules[rule_name]
        rule.input_parameters = input_parameters

    # Initialize evaluation status
    key = (account_id, region)
    if key not in _evaluation_statuses:
        _evaluation_statuses[key] = {}
    _evaluation_statuses[key][rule_name] = {
        "ConfigRuleName": rule_name,
        "ConfigRuleArn": rule_arn,
        "ConfigRuleId": backend.config_rules[rule_name].config_rule_id
        if rule_name in backend.config_rules
        else "",
        "LastSuccessfulInvocationTime": time.time(),
        "FirstActivatedTime": time.time(),
        "LastSuccessfulEvaluationTime": 0,
        "FirstEvaluationStarted": False,
    }

    return {}


def _describe_config_rules(params: dict, region: str, account_id: str) -> dict:
    """DescribeConfigRules - delegates to Moto with proper error for nonexistent rules."""
    backend = _get_config_backend(account_id, region)
    rule_names = params.get("ConfigRuleNames")
    next_token = params.get("NextToken")

    # If specific rule names are requested, check for nonexistent ones first
    if rule_names:
        for name in rule_names:
            if name not in backend.config_rules:
                raise ConfigError(
                    "NoSuchConfigRuleException",
                    "The ConfigRule provided in the request is invalid. "
                    "Please check the configRule name",
                    status=400,
                )

    result = backend.describe_config_rules(rule_names, next_token)

    # Enrich rules with InputParameters that Moto may not serialize
    config_rules = result.get("ConfigRules", [])
    for rule_dict in config_rules:
        rule_name = rule_dict.get("ConfigRuleName", "")
        if rule_name in backend.config_rules:
            rule_obj = backend.config_rules[rule_name]
            if hasattr(rule_obj, "input_parameters") and rule_obj.input_parameters:
                rule_dict["InputParameters"] = rule_obj.input_parameters

    return result


def _describe_compliance_by_config_rule(
    params: dict, region: str, account_id: str
) -> dict:
    """DescribeComplianceByConfigRule - stub returning COMPLIANT for all rules."""
    backend = _get_config_backend(account_id, region)
    rule_names = params.get("ConfigRuleNames", [])
    compliance_types = params.get("ComplianceTypes", [])

    results = []
    rules_to_check = rule_names if rule_names else list(backend.config_rules.keys())

    for rule_name in rules_to_check:
        if rule_name not in backend.config_rules:
            raise ConfigError(
                "NoSuchConfigRuleException",
                "The ConfigRule provided in the request is invalid. "
                "Please check the configRule name",
            )
        compliance_type = "COMPLIANT"
        if compliance_types and compliance_type not in compliance_types:
            continue
        results.append({
            "ConfigRuleName": rule_name,
            "Compliance": {"ComplianceType": compliance_type},
        })

    return {"ComplianceByConfigRules": results}


def _put_evaluations(params: dict, region: str, account_id: str) -> dict:
    """PutEvaluations - handle without requiring TestMode."""
    evaluations = params.get("Evaluations", [])
    result_token = params.get("ResultToken", "")

    if not evaluations:
        raise ConfigError(
            "InvalidParameterValueException",
            "The Evaluations object in your request cannot be null."
            "Add the required parameters and try again.",
        )

    if not result_token:
        raise ConfigError(
            "InvalidResultTokenException",
            "The resultToken provided is invalid.",
        )

    # Store evaluations
    key = (account_id, region)
    if key not in _evaluations:
        _evaluations[key] = {}

    for evaluation in evaluations:
        resource_type = evaluation.get("ComplianceResourceType", "")
        resource_id = evaluation.get("ComplianceResourceId", "")
        eval_key = f"{resource_type}:{resource_id}"
        if eval_key not in _evaluations[key]:
            _evaluations[key][eval_key] = []
        _evaluations[key][eval_key].append(evaluation)

    return {"FailedEvaluations": []}


def _describe_config_rule_evaluation_status(
    params: dict, region: str, account_id: str
) -> dict:
    """DescribeConfigRuleEvaluationStatus - return status for rules."""
    backend = _get_config_backend(account_id, region)
    rule_names = params.get("ConfigRuleNames", [])
    key = (account_id, region)

    if not rule_names:
        rule_names = list(backend.config_rules.keys())

    results = []
    for rule_name in rule_names:
        if rule_name not in backend.config_rules:
            raise ConfigError(
                "NoSuchConfigRuleException",
                "The ConfigRule provided in the request is invalid. "
                "Please check the configRule name",
            )

        rule = backend.config_rules[rule_name]
        status = (_evaluation_statuses.get(key, {}).get(rule_name)) or {
            "ConfigRuleName": rule_name,
            "ConfigRuleArn": rule.config_rule_arn,
            "ConfigRuleId": rule.config_rule_id,
        }

        results.append({
            "ConfigRuleName": rule_name,
            "ConfigRuleArn": rule.config_rule_arn,
            "ConfigRuleId": rule.config_rule_id,
            "LastSuccessfulInvocationTime": status.get(
                "LastSuccessfulInvocationTime", 0
            ),
            "FirstActivatedTime": status.get("FirstActivatedTime", 0),
            "FirstEvaluationStarted": status.get("FirstEvaluationStarted", False),
        })

    return {"ConfigRulesEvaluationStatus": results}


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------


def _error_response(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "message": message})
    return Response(
        content=body,
        status_code=status,
        media_type="application/x-amz-json-1.1",
    )


# ---------------------------------------------------------------------------
# Action dispatch map
# ---------------------------------------------------------------------------

_ACTION_MAP = {
    "PutConfigRule": _put_config_rule,
    "DescribeConfigRules": _describe_config_rules,
    "DescribeComplianceByConfigRule": _describe_compliance_by_config_rule,
    "PutEvaluations": _put_evaluations,
    "DescribeConfigRuleEvaluationStatus": _describe_config_rule_evaluation_status,
}
