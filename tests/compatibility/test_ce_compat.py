"""Cost Explorer (CE) compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def ce():
    return make_client("ce")


class TestCEGetCostAndUsage:
    def test_get_cost_and_usage(self, ce):
        response = ce.get_cost_and_usage(
            TimePeriod={"Start": "2024-01-01", "End": "2024-01-02"},
            Granularity="DAILY",
            Metrics=["BlendedCost"],
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ResultsByTime" in response
        assert isinstance(response["ResultsByTime"], list)

    def test_get_cost_and_usage_monthly_granularity(self, ce):
        response = ce.get_cost_and_usage(
            TimePeriod={"Start": "2024-01-01", "End": "2024-02-01"},
            Granularity="MONTHLY",
            Metrics=["UnblendedCost"],
        )
        assert "ResultsByTime" in response
        assert isinstance(response["ResultsByTime"], list)

    def test_get_cost_and_usage_multiple_metrics(self, ce):
        response = ce.get_cost_and_usage(
            TimePeriod={"Start": "2024-01-01", "End": "2024-01-02"},
            Granularity="DAILY",
            Metrics=["BlendedCost", "UnblendedCost"],
        )
        assert "ResultsByTime" in response
        assert isinstance(response["ResultsByTime"], list)

    def test_get_cost_and_usage_with_group_by(self, ce):
        response = ce.get_cost_and_usage(
            TimePeriod={"Start": "2024-01-01", "End": "2024-01-02"},
            Granularity="DAILY",
            Metrics=["BlendedCost"],
            GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
        )
        assert "ResultsByTime" in response
        assert isinstance(response["ResultsByTime"], list)

    def test_get_cost_and_usage_dimension_values(self, ce):
        """DimensionValueAttributes key is present in response."""
        response = ce.get_cost_and_usage(
            TimePeriod={"Start": "2024-01-01", "End": "2024-01-02"},
            Granularity="DAILY",
            Metrics=["BlendedCost"],
        )
        assert "DimensionValueAttributes" in response


class TestCostCategoryDefinition:
    def _create_cost_category(self, ce, name=None):
        name = name or _unique("cat")
        resp = ce.create_cost_category_definition(
            Name=name,
            RuleVersion="CostCategoryExpression.v1",
            Rules=[
                {
                    "Value": "test-value",
                    "Rule": {"Dimensions": {"Key": "SERVICE", "Values": ["Amazon S3"]}},
                }
            ],
        )
        return resp["CostCategoryArn"]

    def test_create_cost_category_definition(self, ce):
        name = _unique("cat")
        resp = ce.create_cost_category_definition(
            Name=name,
            RuleVersion="CostCategoryExpression.v1",
            Rules=[
                {
                    "Value": "test-value",
                    "Rule": {"Dimensions": {"Key": "SERVICE", "Values": ["Amazon S3"]}},
                }
            ],
        )
        assert "CostCategoryArn" in resp
        assert "arn:aws:ce:" in resp["CostCategoryArn"]
        assert "EffectiveStart" in resp
        ce.delete_cost_category_definition(CostCategoryArn=resp["CostCategoryArn"])

    def test_create_cost_category_with_multiple_rules(self, ce):
        name = _unique("cat")
        resp = ce.create_cost_category_definition(
            Name=name,
            RuleVersion="CostCategoryExpression.v1",
            Rules=[
                {
                    "Value": "storage",
                    "Rule": {"Dimensions": {"Key": "SERVICE", "Values": ["Amazon S3"]}},
                },
                {
                    "Value": "compute",
                    "Rule": {"Dimensions": {"Key": "SERVICE", "Values": ["Amazon EC2"]}},
                },
            ],
        )
        arn = resp["CostCategoryArn"]
        try:
            desc = ce.describe_cost_category_definition(CostCategoryArn=arn)
            assert len(desc["CostCategory"]["Rules"]) == 2
        finally:
            ce.delete_cost_category_definition(CostCategoryArn=arn)

    def test_describe_cost_category_definition(self, ce):
        arn = self._create_cost_category(ce)
        try:
            result = ce.describe_cost_category_definition(CostCategoryArn=arn)
            assert "CostCategory" in result
            assert result["CostCategory"]["CostCategoryArn"] == arn
            assert result["CostCategory"]["RuleVersion"] == "CostCategoryExpression.v1"
        finally:
            ce.delete_cost_category_definition(CostCategoryArn=arn)

    def test_describe_cost_category_fields(self, ce):
        """Describe returns Name, EffectiveStart, and Rules."""
        name = _unique("cat")
        arn = self._create_cost_category(ce, name=name)
        try:
            result = ce.describe_cost_category_definition(CostCategoryArn=arn)
            cat = result["CostCategory"]
            assert cat["Name"] == name
            assert "EffectiveStart" in cat
            assert isinstance(cat["Rules"], list)
            assert len(cat["Rules"]) >= 1
        finally:
            ce.delete_cost_category_definition(CostCategoryArn=arn)

    def test_describe_nonexistent_cost_category(self, ce):
        fake_arn = "arn:aws:ce::123456789012:costcategory/00000000-0000-0000-0000-000000000000"
        with pytest.raises(ClientError) as exc:
            ce.describe_cost_category_definition(CostCategoryArn=fake_arn)
        assert "ResourceNotFoundException" in exc.value.response["Error"]["Code"]

    def test_update_cost_category_definition(self, ce):
        arn = self._create_cost_category(ce)
        try:
            update_resp = ce.update_cost_category_definition(
                CostCategoryArn=arn,
                RuleVersion="CostCategoryExpression.v1",
                Rules=[
                    {
                        "Value": "updated-value",
                        "Rule": {"Dimensions": {"Key": "SERVICE", "Values": ["Amazon EC2"]}},
                    }
                ],
            )
            assert update_resp["CostCategoryArn"] == arn
            assert "EffectiveStart" in update_resp

            # Verify updated value
            desc = ce.describe_cost_category_definition(CostCategoryArn=arn)
            assert desc["CostCategory"]["Rules"][0]["Value"] == "updated-value"
        finally:
            ce.delete_cost_category_definition(CostCategoryArn=arn)

    def test_delete_cost_category_definition(self, ce):
        arn = self._create_cost_category(ce)
        result = ce.delete_cost_category_definition(CostCategoryArn=arn)
        assert "CostCategoryArn" in result
        assert result["CostCategoryArn"] == arn

    def test_delete_then_describe_raises(self, ce):
        """After deletion, describe should raise ResourceNotFoundException."""
        arn = self._create_cost_category(ce)
        ce.delete_cost_category_definition(CostCategoryArn=arn)
        with pytest.raises(ClientError) as exc:
            ce.describe_cost_category_definition(CostCategoryArn=arn)
        assert "ResourceNotFoundException" in exc.value.response["Error"]["Code"]


class TestCETagOperations:
    def _create_cost_category(self, ce):
        name = _unique("cat")
        resp = ce.create_cost_category_definition(
            Name=name,
            RuleVersion="CostCategoryExpression.v1",
            Rules=[
                {
                    "Value": "v1",
                    "Rule": {"Dimensions": {"Key": "SERVICE", "Values": ["Amazon S3"]}},
                }
            ],
        )
        return resp["CostCategoryArn"]

    def test_tag_resource(self, ce):
        arn = self._create_cost_category(ce)
        try:
            ce.tag_resource(
                ResourceArn=arn,
                ResourceTags=[{"Key": "env", "Value": "prod"}],
            )
            tags_resp = ce.list_tags_for_resource(ResourceArn=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["ResourceTags"]}
            assert tag_map["env"] == "prod"
        finally:
            ce.delete_cost_category_definition(CostCategoryArn=arn)

    def test_tag_resource_multiple_tags(self, ce):
        arn = self._create_cost_category(ce)
        try:
            ce.tag_resource(
                ResourceArn=arn,
                ResourceTags=[
                    {"Key": "env", "Value": "staging"},
                    {"Key": "team", "Value": "backend"},
                ],
            )
            tags_resp = ce.list_tags_for_resource(ResourceArn=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["ResourceTags"]}
            assert tag_map["env"] == "staging"
            assert tag_map["team"] == "backend"
        finally:
            ce.delete_cost_category_definition(CostCategoryArn=arn)

    def test_untag_resource(self, ce):
        arn = self._create_cost_category(ce)
        try:
            ce.tag_resource(
                ResourceArn=arn,
                ResourceTags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "dev"},
                ],
            )
            ce.untag_resource(ResourceArn=arn, ResourceTagKeys=["team"])
            tags_resp = ce.list_tags_for_resource(ResourceArn=arn)
            tag_keys = [t["Key"] for t in tags_resp["ResourceTags"]]
            assert "env" in tag_keys
            assert "team" not in tag_keys
        finally:
            ce.delete_cost_category_definition(CostCategoryArn=arn)

    def test_list_tags_for_resource_empty(self, ce):
        arn = self._create_cost_category(ce)
        try:
            tags_resp = ce.list_tags_for_resource(ResourceArn=arn)
            assert "ResourceTags" in tags_resp
            assert isinstance(tags_resp["ResourceTags"], list)
        finally:
            ce.delete_cost_category_definition(CostCategoryArn=arn)

    def test_tag_overwrite_existing(self, ce):
        """Tagging with same key overwrites the value."""
        arn = self._create_cost_category(ce)
        try:
            ce.tag_resource(
                ResourceArn=arn,
                ResourceTags=[{"Key": "env", "Value": "dev"}],
            )
            ce.tag_resource(
                ResourceArn=arn,
                ResourceTags=[{"Key": "env", "Value": "prod"}],
            )
            tags_resp = ce.list_tags_for_resource(ResourceArn=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["ResourceTags"]}
            assert tag_map["env"] == "prod"
        finally:
            ce.delete_cost_category_definition(CostCategoryArn=arn)
