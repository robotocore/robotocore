"""Cost Explorer (CE) compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def ce():
    return make_client("ce")


class TestCEOperations:
    def test_get_cost_and_usage(self, ce):
        response = ce.get_cost_and_usage(
            TimePeriod={"Start": "2024-01-01", "End": "2024-01-02"},
            Granularity="DAILY",
            Metrics=["BlendedCost"],
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ResultsByTime" in response
        assert isinstance(response["ResultsByTime"], list)


class TestCostCategoryDefinition:
    def _create_cost_category(self, ce):
        resp = ce.create_cost_category_definition(
            Name="test-category",
            RuleVersion="CostCategoryExpression.v1",
            Rules=[
                {
                    "Value": "test-value",
                    "Rule": {"Dimensions": {"Key": "SERVICE", "Values": ["Amazon S3"]}},
                }
            ],
        )
        return resp["CostCategoryArn"]

    def test_describe_cost_category_definition(self, ce):
        arn = self._create_cost_category(ce)
        try:
            result = ce.describe_cost_category_definition(CostCategoryArn=arn)
            assert "CostCategory" in result
            assert result["CostCategory"]["CostCategoryArn"] == arn
            assert result["CostCategory"]["Name"] == "test-category"
        finally:
            ce.delete_cost_category_definition(CostCategoryArn=arn)

    def test_delete_cost_category_definition(self, ce):
        arn = self._create_cost_category(ce)
        result = ce.delete_cost_category_definition(CostCategoryArn=arn)
        assert "CostCategoryArn" in result
        assert result["CostCategoryArn"] == arn
