"""Unit tests for the Config native provider."""

import asyncio
import copy
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robotocore.services.config.provider import (
    ConfigError,
    _delete_config_rule,
    _describe_compliance_by_config_rule,
    _describe_config_rule_evaluation_status,
    _describe_config_rules,
    _evaluation_statuses,
    _evaluations,
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
        asyncio.run(handle_config_request(request, "us-east-1", "123456789012"))
        mock_forward.assert_called_once()


class TestCategoricalBugs:
    """Tests for categorical bug patterns that likely exist across many providers."""

    # -----------------------------------------------------------------------
    # Bug 1: Input mutation — _put_config_rule mutates the caller's params dict
    # Category: Any provider that strips/modifies params before forwarding to Moto
    # -----------------------------------------------------------------------

    @patch("robotocore.services.config.provider._get_config_backend")
    def test_put_config_rule_does_not_mutate_caller_params(self, mock_backend_fn):
        """Callers should be able to inspect their params dict after the call."""
        mock_backend = MagicMock()
        mock_backend.put_config_rule.return_value = "arn:aws:config:us-east-1:123:rule/123"
        mock_rule = MagicMock()
        mock_rule.config_rule_id = "config-rule-123"
        mock_rule.config_rule_arn = "arn:aws:config:us-east-1:123:rule/123"
        mock_backend.config_rules = {"test-rule": mock_rule}
        mock_backend_fn.return_value = mock_backend

        params = {
            "ConfigRule": {
                "ConfigRuleName": "test-rule",
                "Source": {"Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED"},
                "InputParameters": '{"maxDays": "90"}',
            }
        }
        original = copy.deepcopy(params)
        _put_config_rule(params, "us-east-1", "123456789012")

        # The caller's dict should not have been mutated
        expected = original["ConfigRule"]["InputParameters"]
        assert params["ConfigRule"].get("InputParameters") == expected

    @patch("robotocore.services.config.provider._get_config_backend")
    def test_put_custom_lambda_rule_does_not_mutate_source(self, mock_backend_fn):
        """CUSTOM_LAMBDA source fields should not be permanently mutated."""
        mock_backend = MagicMock()
        mock_backend.put_config_rule.return_value = "arn:aws:config:us-east-1:123:rule/123"
        mock_rule = MagicMock()
        mock_rule.config_rule_id = "config-rule-123"
        mock_rule.config_rule_arn = "arn:aws:config:us-east-1:123:rule/123"
        mock_backend.config_rules = {"custom-rule": mock_rule}
        mock_backend_fn.return_value = mock_backend

        params = {
            "ConfigRule": {
                "ConfigRuleName": "custom-rule",
                "Source": {
                    "Owner": "CUSTOM_LAMBDA",
                    "SourceIdentifier": "arn:aws:lambda:us-east-1:123:function:my-func",
                    "SourceDetails": [{"EventSource": "aws.config"}],
                },
            }
        }
        original_source = copy.deepcopy(params["ConfigRule"]["Source"])
        _put_config_rule(params, "us-east-1", "123456789012")

        # The caller's source dict should be restored
        assert params["ConfigRule"]["Source"]["Owner"] == original_source["Owner"]
        assert (
            params["ConfigRule"]["Source"]["SourceIdentifier"]
            == original_source["SourceIdentifier"]
        )

    # -----------------------------------------------------------------------
    # Bug 2: CUSTOM_LAMBDA source not restored if Moto raises
    # Category: Any provider that modifies state before delegating, then restores
    # -----------------------------------------------------------------------

    @patch("robotocore.services.config.provider._get_config_backend")
    def test_put_custom_lambda_rule_restores_source_on_moto_error(self, mock_backend_fn):
        """If Moto raises, the caller's params should still be intact."""
        mock_backend = MagicMock()
        mock_backend.put_config_rule.side_effect = Exception("Moto blew up")
        mock_backend.config_rules = {}
        mock_backend_fn.return_value = mock_backend

        params = {
            "ConfigRule": {
                "ConfigRuleName": "custom-rule",
                "Source": {
                    "Owner": "CUSTOM_LAMBDA",
                    "SourceIdentifier": "arn:aws:lambda:us-east-1:123:function:my-func",
                    "SourceDetails": [{"EventSource": "aws.config"}],
                },
            }
        }
        original_source = copy.deepcopy(params["ConfigRule"]["Source"])

        with pytest.raises(Exception, match="Moto blew up"):
            _put_config_rule(params, "us-east-1", "123456789012")

        # Even after an error, the source dict should not be left in a corrupted state
        assert params["ConfigRule"]["Source"]["Owner"] == original_source["Owner"]
        assert (
            params["ConfigRule"]["Source"]["SourceIdentifier"]
            == original_source["SourceIdentifier"]
        )

    # -----------------------------------------------------------------------
    # Bug 3: Global state not cleaned up on delete (parent-child cascade)
    # Category: Any provider with in-memory stores that has no delete handler
    # -----------------------------------------------------------------------

    @patch("robotocore.services.config.provider._get_config_backend")
    def test_delete_config_rule_cleans_up_evaluation_status(self, mock_backend_fn):
        """Deleting a rule should remove its evaluation status from the global store."""
        mock_backend = MagicMock()
        mock_backend.put_config_rule.return_value = "arn:aws:config:us-east-1:123:rule/123"
        mock_rule = MagicMock()
        mock_rule.config_rule_id = "config-rule-123"
        mock_rule.config_rule_arn = "arn:aws:config:us-east-1:123:rule/123"
        mock_backend.config_rules = {"orphan-rule": mock_rule}
        mock_backend.delete_config_rule = MagicMock()
        mock_backend_fn.return_value = mock_backend

        # Create a rule (populates _evaluation_statuses)
        _put_config_rule(
            {
                "ConfigRule": {
                    "ConfigRuleName": "orphan-rule",
                    "Source": {"Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED"},
                }
            },
            "us-east-1",
            "123456789012",
        )

        key = ("123456789012", "us-east-1")
        assert "orphan-rule" in _evaluation_statuses.get(key, {})

        # Delete the rule — our provider must clean up in-memory state
        _delete_config_rule({"ConfigRuleName": "orphan-rule"}, "us-east-1", "123456789012")

        # Moto's delete should have been called
        mock_backend.delete_config_rule.assert_called_once_with("orphan-rule")

        # Evaluation status should be cleaned up
        assert "orphan-rule" not in _evaluation_statuses.get(key, {})

    # -----------------------------------------------------------------------
    # Bug 4: Global state leaks between tests/accounts (no isolation)
    # Category: Module-level mutable dicts in any provider
    # -----------------------------------------------------------------------

    def test_evaluations_are_isolated_by_account_and_region(self):
        """Evaluations stored for one account/region must not appear in another."""
        # Clear global state
        _evaluations.clear()

        _put_evaluations(
            {
                "Evaluations": [
                    {
                        "ComplianceResourceType": "AWS::S3::Bucket",
                        "ComplianceResourceId": "bucket-1",
                        "ComplianceType": "COMPLIANT",
                    }
                ],
                "ResultToken": "tok-1",
            },
            "us-east-1",
            "111111111111",
        )

        # Different account should have no data
        key_other = ("222222222222", "us-east-1")
        assert key_other not in _evaluations

        # Different region should have no data
        key_other_region = ("111111111111", "eu-west-1")
        assert key_other_region not in _evaluations

    # -----------------------------------------------------------------------
    # Bug 5: _describe_config_rule_evaluation_status for nonexistent rule
    # after put + delete should not crash
    # -----------------------------------------------------------------------

    @patch("robotocore.services.config.provider._get_config_backend")
    def test_evaluation_status_nonexistent_rule_raises(self, mock_backend_fn):
        """Requesting status for a nonexistent rule should raise NoSuchConfigRuleException."""
        mock_backend = MagicMock()
        mock_backend.config_rules = {}
        mock_backend_fn.return_value = mock_backend

        with pytest.raises(ConfigError) as exc:
            _describe_config_rule_evaluation_status(
                {"ConfigRuleNames": ["no-such-rule"]}, "us-east-1", "123456789012"
            )
        assert exc.value.code == "NoSuchConfigRuleException"
