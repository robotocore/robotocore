"""Unit tests for the Config native provider."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robotocore.services.config.provider import (
    ConfigError,
    _describe_compliance_by_config_rule,
    _describe_config_rules,
    _put_config_rule,
    _put_evaluations,
    handle_config_request,
)


def _make_request(body: dict | None = None, target: str = ""):
    request = MagicMock()
    raw = json.dumps(body).encode() if body else b""
    request.body = AsyncMock(return_value=raw)
    request.headers = {"x-amz-target": target}
    request.method = "POST"
    request.url = MagicMock()
    request.url.path = "/"
    request.url.query = None
    return request


class TestConfigProvider:
    @patch("robotocore.services.config.provider._get_config_backend")
    def test_put_config_rule_strips_input_parameters(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend.put_config_rule.return_value = "arn:aws:config:us-east-1:123:config-rule/123"
        mock_backend.config_rules = {}
        mock_backend_fn.return_value = mock_backend

        params = {
            "ConfigRule": {
                "ConfigRuleName": "test-rule",
                "Source": {"Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED"},
                "InputParameters": '{"maxDays": "90"}',
            }
        }
        result = _put_config_rule(params, "us-east-1", "123456789012")
        assert result == {}

        # Verify InputParameters was stripped before calling Moto
        call_args = mock_backend.put_config_rule.call_args[0][0]
        assert "InputParameters" not in call_args

    @patch("robotocore.services.config.provider._get_config_backend")
    def test_describe_nonexistent_rule_raises(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend.config_rules = {}
        mock_backend_fn.return_value = mock_backend

        with pytest.raises(ConfigError) as exc:
            _describe_config_rules(
                {"ConfigRuleNames": ["nonexistent"]}, "us-east-1", "123456789012"
            )
        assert exc.value.code == "NoSuchConfigRuleException"

    @patch("robotocore.services.config.provider._get_config_backend")
    def test_describe_compliance_returns_compliant(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend.config_rules = {"my-rule": MagicMock()}
        mock_backend_fn.return_value = mock_backend

        result = _describe_compliance_by_config_rule({}, "us-east-1", "123456789012")
        assert "ComplianceByConfigRules" in result
        assert len(result["ComplianceByConfigRules"]) == 1
        assert result["ComplianceByConfigRules"][0]["ConfigRuleName"] == "my-rule"

    def test_put_evaluations_requires_evaluations(self):
        with pytest.raises(ConfigError) as exc:
            _put_evaluations({"Evaluations": [], "ResultToken": "tok"}, "us-east-1", "123456789012")
        assert exc.value.code == "InvalidParameterValueException"

    def test_put_evaluations_requires_result_token(self):
        with pytest.raises(ConfigError) as exc:
            _put_evaluations(
                {"Evaluations": [{"ComplianceResourceType": "AWS::S3::Bucket"}]},
                "us-east-1",
                "123456789012",
            )
        assert exc.value.code == "InvalidResultTokenException"

    def test_put_evaluations_success(self):
        result = _put_evaluations(
            {
                "Evaluations": [
                    {
                        "ComplianceResourceType": "AWS::S3::Bucket",
                        "ComplianceResourceId": "my-bucket",
                        "ComplianceType": "COMPLIANT",
                    }
                ],
                "ResultToken": "test-token",
            },
            "us-east-1",
            "123456789012",
        )
        assert result == {"FailedEvaluations": []}

    @patch("robotocore.services.config.provider.forward_to_moto")
    def test_unknown_action_forwards_to_moto(self, mock_forward):
        mock_forward.return_value = MagicMock(status_code=200)
        request = _make_request(
            body={"ConfigurationRecorderName": "default"},
            target="StarlingDoveService.DescribeConfigurationRecorders",
        )
        asyncio.get_event_loop().run_until_complete(
            handle_config_request(request, "us-east-1", "123456789012")
        )
        mock_forward.assert_called_once()
