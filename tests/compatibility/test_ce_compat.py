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


class TestCEAnomalies:
    """Tests for CE anomaly monitoring operations."""

    def test_get_anomaly_monitors_empty(self, ce):
        """GetAnomalyMonitors returns AnomalyMonitors key."""
        resp = ce.get_anomaly_monitors()
        assert "AnomalyMonitors" in resp
        assert isinstance(resp["AnomalyMonitors"], list)

    def test_get_anomaly_subscriptions_empty(self, ce):
        """GetAnomalySubscriptions returns AnomalySubscriptions key."""
        resp = ce.get_anomaly_subscriptions()
        assert "AnomalySubscriptions" in resp
        assert isinstance(resp["AnomalySubscriptions"], list)

    def test_get_anomalies(self, ce):
        """GetAnomalies returns Anomalies key."""
        resp = ce.get_anomalies(
            DateInterval={"StartDate": "2024-01-01", "EndDate": "2024-01-31"},
        )
        assert "Anomalies" in resp
        assert isinstance(resp["Anomalies"], list)


class TestCECostQueries:
    """Tests for CE cost query operations."""

    def test_get_cost_categories(self, ce):
        """GetCostCategories returns expected keys."""
        resp = ce.get_cost_categories(
            TimePeriod={"Start": "2024-01-01", "End": "2024-01-31"},
        )
        assert "CostCategoryNames" in resp
        assert "ReturnSize" in resp
        assert "TotalSize" in resp

    def test_get_tags(self, ce):
        """GetTags returns tags list and size info."""
        resp = ce.get_tags(
            TimePeriod={"Start": "2024-01-01", "End": "2024-01-31"},
        )
        assert "Tags" in resp
        assert isinstance(resp["Tags"], list)
        assert "ReturnSize" in resp
        assert "TotalSize" in resp

    def test_get_dimension_values(self, ce):
        """GetDimensionValues returns dimension values for SERVICE."""
        resp = ce.get_dimension_values(
            TimePeriod={"Start": "2024-01-01", "End": "2024-01-31"},
            Dimension="SERVICE",
        )
        assert "DimensionValues" in resp
        assert isinstance(resp["DimensionValues"], list)
        assert "ReturnSize" in resp

    def test_get_cost_forecast(self, ce):
        """GetCostForecast returns Total and ForecastResultsByTime."""
        resp = ce.get_cost_forecast(
            TimePeriod={"Start": "2025-01-01", "End": "2025-01-31"},
            Metric="BLENDED_COST",
            Granularity="MONTHLY",
        )
        assert "Total" in resp
        assert "ForecastResultsByTime" in resp
        assert isinstance(resp["ForecastResultsByTime"], list)

    def test_get_usage_forecast(self, ce):
        """GetUsageForecast returns Total and ForecastResultsByTime."""
        resp = ce.get_usage_forecast(
            TimePeriod={"Start": "2025-01-01", "End": "2025-01-31"},
            Metric="USAGE_QUANTITY",
            Granularity="MONTHLY",
        )
        assert "Total" in resp
        assert "ForecastResultsByTime" in resp

    def test_get_cost_and_usage_with_resources(self, ce):
        """GetCostAndUsageWithResources returns ResultsByTime."""
        resp = ce.get_cost_and_usage_with_resources(
            TimePeriod={"Start": "2024-01-01", "End": "2024-01-31"},
            Granularity="MONTHLY",
            Filter={"Dimensions": {"Key": "SERVICE", "Values": ["Amazon S3"]}},
        )
        assert "ResultsByTime" in resp
        assert isinstance(resp["ResultsByTime"], list)

    def test_get_cost_and_usage_comparisons(self, ce):
        """GetCostAndUsageComparisons returns comparison data."""
        resp = ce.get_cost_and_usage_comparisons(
            BaselineTimePeriod={"Start": "2024-01-01", "End": "2024-01-31"},
            ComparisonTimePeriod={"Start": "2024-02-01", "End": "2024-02-28"},
            MetricForComparison="UNBLENDED_COST",
        )
        assert "CostAndUsageComparisons" in resp

    def test_get_cost_comparison_drivers(self, ce):
        """GetCostComparisonDrivers returns driver data."""
        resp = ce.get_cost_comparison_drivers(
            BaselineTimePeriod={"Start": "2024-01-01", "End": "2024-01-31"},
            ComparisonTimePeriod={"Start": "2024-02-01", "End": "2024-02-28"},
            MetricForComparison="UNBLENDED_COST",
        )
        assert "CostComparisonDrivers" in resp

    def test_get_approximate_usage_records(self, ce):
        """GetApproximateUsageRecords returns service-level usage."""
        resp = ce.get_approximate_usage_records(
            Granularity="MONTHLY",
            ApproximationDimension="SERVICE",
        )
        assert "TotalRecords" in resp
        assert "LookbackPeriod" in resp


class TestCEReservations:
    """Tests for CE reservation-related operations."""

    def test_get_reservation_coverage(self, ce):
        """GetReservationCoverage returns coverage data."""
        resp = ce.get_reservation_coverage(
            TimePeriod={"Start": "2024-01-01", "End": "2024-01-31"},
        )
        assert "CoveragesByTime" in resp
        assert isinstance(resp["CoveragesByTime"], list)
        assert "Total" in resp

    def test_get_reservation_utilization(self, ce):
        """GetReservationUtilization returns utilization data."""
        resp = ce.get_reservation_utilization(
            TimePeriod={"Start": "2024-01-01", "End": "2024-01-31"},
        )
        assert "UtilizationsByTime" in resp
        assert isinstance(resp["UtilizationsByTime"], list)
        assert "Total" in resp

    def test_get_reservation_purchase_recommendation(self, ce):
        """GetReservationPurchaseRecommendation returns recommendations."""
        resp = ce.get_reservation_purchase_recommendation(Service="Amazon EC2")
        assert "Recommendations" in resp
        assert isinstance(resp["Recommendations"], list)

    def test_get_rightsizing_recommendation(self, ce):
        """GetRightsizingRecommendation returns recommendations."""
        resp = ce.get_rightsizing_recommendation(Service="Amazon EC2")
        assert "RightsizingRecommendations" in resp
        assert isinstance(resp["RightsizingRecommendations"], list)


class TestCESavingsPlans:
    """Tests for CE Savings Plans operations."""

    def test_get_savings_plans_coverage(self, ce):
        """GetSavingsPlansCoverage returns coverage data."""
        resp = ce.get_savings_plans_coverage(
            TimePeriod={"Start": "2024-01-01", "End": "2024-01-31"},
        )
        assert "SavingsPlansCoverages" in resp
        assert isinstance(resp["SavingsPlansCoverages"], list)

    def test_get_savings_plans_utilization(self, ce):
        """GetSavingsPlansUtilization returns utilization Total."""
        resp = ce.get_savings_plans_utilization(
            TimePeriod={"Start": "2024-01-01", "End": "2024-01-31"},
        )
        assert "Total" in resp

    def test_get_savings_plans_utilization_details(self, ce):
        """GetSavingsPlansUtilizationDetails returns detail list."""
        resp = ce.get_savings_plans_utilization_details(
            TimePeriod={"Start": "2024-01-01", "End": "2024-01-31"},
        )
        assert "SavingsPlansUtilizationDetails" in resp
        assert isinstance(resp["SavingsPlansUtilizationDetails"], list)

    def test_get_savings_plans_purchase_recommendation(self, ce):
        """GetSavingsPlansPurchaseRecommendation returns a response."""
        resp = ce.get_savings_plans_purchase_recommendation(
            SavingsPlansType="COMPUTE_SP",
            TermInYears="ONE_YEAR",
            PaymentOption="NO_UPFRONT",
            LookbackPeriodInDays="THIRTY_DAYS",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestCEListOperations:
    """Tests for CE list operations."""

    def test_list_cost_category_definitions(self, ce):
        """ListCostCategoryDefinitions returns references list."""
        resp = ce.list_cost_category_definitions()
        assert "CostCategoryReferences" in resp
        assert isinstance(resp["CostCategoryReferences"], list)

    def test_list_cost_allocation_tags(self, ce):
        """ListCostAllocationTags returns tags list."""
        resp = ce.list_cost_allocation_tags()
        assert "CostAllocationTags" in resp
        assert isinstance(resp["CostAllocationTags"], list)

    def test_list_cost_allocation_tag_backfill_history(self, ce):
        """ListCostAllocationTagBackfillHistory returns backfill requests."""
        resp = ce.list_cost_allocation_tag_backfill_history()
        assert "BackfillRequests" in resp
        assert isinstance(resp["BackfillRequests"], list)

    def test_list_savings_plans_purchase_recommendation_generation(self, ce):
        """ListSavingsPlansPurchaseRecommendationGeneration returns generation list."""
        resp = ce.list_savings_plans_purchase_recommendation_generation()
        assert "GenerationSummaryList" in resp
        assert isinstance(resp["GenerationSummaryList"], list)

    def test_list_commitment_purchase_analyses(self, ce):
        """ListCommitmentPurchaseAnalyses returns analysis list."""
        resp = ce.list_commitment_purchase_analyses()
        assert "AnalysisSummaryList" in resp
        assert isinstance(resp["AnalysisSummaryList"], list)

    def test_list_cost_category_resource_associations(self, ce):
        """ListCostCategoryResourceAssociations with a cost category ARN."""
        # Create a cost category first
        name = _unique("cat")
        create_resp = ce.create_cost_category_definition(
            Name=name,
            RuleVersion="CostCategoryExpression.v1",
            Rules=[
                {
                    "Value": "v1",
                    "Rule": {"Dimensions": {"Key": "SERVICE", "Values": ["Amazon S3"]}},
                }
            ],
        )
        arn = create_resp["CostCategoryArn"]
        try:
            resp = ce.list_cost_category_resource_associations(CostCategoryArn=arn)
            assert "CostCategoryResourceAssociations" in resp
        finally:
            ce.delete_cost_category_definition(CostCategoryArn=arn)


class TestCEAnomalyMonitorCRUD:
    """Tests for anomaly monitor create/update/delete operations."""

    def test_create_anomaly_monitor(self, ce):
        """CreateAnomalyMonitor returns a MonitorArn."""
        resp = ce.create_anomaly_monitor(
            AnomalyMonitor={
                "MonitorName": _unique("mon"),
                "MonitorType": "DIMENSIONAL",
                "MonitorDimension": "SERVICE",
            }
        )
        assert "MonitorArn" in resp
        assert "arn:aws:ce:" in resp["MonitorArn"]
        ce.delete_anomaly_monitor(MonitorArn=resp["MonitorArn"])

    def test_create_anomaly_monitor_custom(self, ce):
        """CreateAnomalyMonitor with CUSTOM type returns MonitorArn."""
        resp = ce.create_anomaly_monitor(
            AnomalyMonitor={
                "MonitorName": _unique("custom-mon"),
                "MonitorType": "CUSTOM",
                "MonitorSpecification": {"Dimensions": {"Key": "SERVICE", "Values": ["Amazon S3"]}},
            }
        )
        assert "MonitorArn" in resp
        ce.delete_anomaly_monitor(MonitorArn=resp["MonitorArn"])

    def test_update_anomaly_monitor(self, ce):
        """UpdateAnomalyMonitor returns the updated MonitorArn."""
        create_resp = ce.create_anomaly_monitor(
            AnomalyMonitor={
                "MonitorName": _unique("mon"),
                "MonitorType": "DIMENSIONAL",
                "MonitorDimension": "SERVICE",
            }
        )
        arn = create_resp["MonitorArn"]
        try:
            update_resp = ce.update_anomaly_monitor(MonitorArn=arn, MonitorName="updated-monitor")
            assert update_resp["MonitorArn"] == arn
        finally:
            ce.delete_anomaly_monitor(MonitorArn=arn)

    def test_delete_anomaly_monitor(self, ce):
        """DeleteAnomalyMonitor returns 200."""
        create_resp = ce.create_anomaly_monitor(
            AnomalyMonitor={
                "MonitorName": _unique("mon"),
                "MonitorType": "DIMENSIONAL",
                "MonitorDimension": "SERVICE",
            }
        )
        arn = create_resp["MonitorArn"]
        del_resp = ce.delete_anomaly_monitor(MonitorArn=arn)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_then_list_anomaly_monitors(self, ce):
        """Created monitor appears in GetAnomalyMonitors."""
        name = _unique("mon")
        create_resp = ce.create_anomaly_monitor(
            AnomalyMonitor={
                "MonitorName": name,
                "MonitorType": "DIMENSIONAL",
                "MonitorDimension": "SERVICE",
            }
        )
        arn = create_resp["MonitorArn"]
        try:
            list_resp = ce.get_anomaly_monitors()
            arns = [m["MonitorArn"] for m in list_resp["AnomalyMonitors"]]
            assert arn in arns
        finally:
            ce.delete_anomaly_monitor(MonitorArn=arn)


class TestCEAnomalySubscriptionCRUD:
    """Tests for anomaly subscription create/update/delete operations."""

    def _create_monitor(self, ce):
        resp = ce.create_anomaly_monitor(
            AnomalyMonitor={
                "MonitorName": _unique("mon"),
                "MonitorType": "DIMENSIONAL",
                "MonitorDimension": "SERVICE",
            }
        )
        return resp["MonitorArn"]

    def test_create_anomaly_subscription(self, ce):
        """CreateAnomalySubscription returns a SubscriptionArn."""
        mon_arn = self._create_monitor(ce)
        try:
            resp = ce.create_anomaly_subscription(
                AnomalySubscription={
                    "MonitorArnList": [mon_arn],
                    "Subscribers": [{"Address": "test@example.com", "Type": "EMAIL"}],
                    "Frequency": "DAILY",
                    "SubscriptionName": _unique("sub"),
                }
            )
            assert "SubscriptionArn" in resp
            assert "arn:aws:ce:" in resp["SubscriptionArn"]
            ce.delete_anomaly_subscription(SubscriptionArn=resp["SubscriptionArn"])
        finally:
            ce.delete_anomaly_monitor(MonitorArn=mon_arn)

    def test_update_anomaly_subscription(self, ce):
        """UpdateAnomalySubscription returns the updated SubscriptionArn."""
        mon_arn = self._create_monitor(ce)
        try:
            sub_resp = ce.create_anomaly_subscription(
                AnomalySubscription={
                    "MonitorArnList": [mon_arn],
                    "Subscribers": [{"Address": "test@example.com", "Type": "EMAIL"}],
                    "Frequency": "DAILY",
                    "SubscriptionName": _unique("sub"),
                }
            )
            sub_arn = sub_resp["SubscriptionArn"]
            try:
                update_resp = ce.update_anomaly_subscription(
                    SubscriptionArn=sub_arn, SubscriptionName="updated-sub"
                )
                assert update_resp["SubscriptionArn"] == sub_arn
            finally:
                ce.delete_anomaly_subscription(SubscriptionArn=sub_arn)
        finally:
            ce.delete_anomaly_monitor(MonitorArn=mon_arn)

    def test_delete_anomaly_subscription(self, ce):
        """DeleteAnomalySubscription returns 200."""
        mon_arn = self._create_monitor(ce)
        try:
            sub_resp = ce.create_anomaly_subscription(
                AnomalySubscription={
                    "MonitorArnList": [mon_arn],
                    "Subscribers": [{"Address": "test@example.com", "Type": "EMAIL"}],
                    "Frequency": "DAILY",
                    "SubscriptionName": _unique("sub"),
                }
            )
            del_resp = ce.delete_anomaly_subscription(SubscriptionArn=sub_resp["SubscriptionArn"])
            assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            ce.delete_anomaly_monitor(MonitorArn=mon_arn)

    def test_create_then_list_anomaly_subscriptions(self, ce):
        """Created subscription appears in GetAnomalySubscriptions."""
        mon_arn = self._create_monitor(ce)
        try:
            sub_resp = ce.create_anomaly_subscription(
                AnomalySubscription={
                    "MonitorArnList": [mon_arn],
                    "Subscribers": [{"Address": "test@example.com", "Type": "EMAIL"}],
                    "Frequency": "DAILY",
                    "SubscriptionName": _unique("sub"),
                }
            )
            sub_arn = sub_resp["SubscriptionArn"]
            try:
                list_resp = ce.get_anomaly_subscriptions()
                arns = [s["SubscriptionArn"] for s in list_resp["AnomalySubscriptions"]]
                assert sub_arn in arns
            finally:
                ce.delete_anomaly_subscription(SubscriptionArn=sub_arn)
        finally:
            ce.delete_anomaly_monitor(MonitorArn=mon_arn)


class TestCEAnomalyFeedback:
    """Tests for ProvideAnomalyFeedback."""

    def test_provide_anomaly_feedback(self, ce):
        """ProvideAnomalyFeedback returns AnomalyId."""
        resp = ce.provide_anomaly_feedback(AnomalyId="fake-anomaly-id", Feedback="YES")
        assert "AnomalyId" in resp
        assert resp["AnomalyId"] == "fake-anomaly-id"

    def test_provide_anomaly_feedback_no(self, ce):
        """ProvideAnomalyFeedback with NO feedback."""
        resp = ce.provide_anomaly_feedback(AnomalyId="another-anomaly-id", Feedback="NO")
        assert resp["AnomalyId"] == "another-anomaly-id"


class TestCERecommendationGeneration:
    """Tests for StartSavingsPlansPurchaseRecommendationGeneration."""

    def test_start_savings_plans_purchase_recommendation_generation(self, ce):
        """StartSavingsPlansPurchaseRecommendationGeneration returns IDs."""
        resp = ce.start_savings_plans_purchase_recommendation_generation()
        assert "RecommendationId" in resp
        assert "GenerationStartedTime" in resp
        assert "EstimatedCompletionTime" in resp


class TestCEAdditionalOps:
    """Tests for additional Cost Explorer operations."""

    def test_start_commitment_purchase_analysis(self, ce):
        """StartCommitmentPurchaseAnalysis returns analysis ID."""
        resp = ce.start_commitment_purchase_analysis(
            CommitmentPurchaseAnalysisConfiguration={
                "SavingsPlansPurchaseAnalysisConfiguration": {
                    "AccountScope": "PAYER",
                    "AnalysisType": "MAX_SAVINGS",
                    "SavingsPlansToAdd": [
                        {
                            "PaymentOption": "NO_UPFRONT",
                            "SavingsPlansType": "COMPUTE_SP",
                            "TermInYears": "ONE_YEAR",
                            "OfferingId": "fake-offering-id",
                            "SavingsPlansCommitment": 1.0,
                        }
                    ],
                    "LookBackTimePeriod": {
                        "Start": "2024-01-01",
                        "End": "2024-12-31",
                    },
                }
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AnalysisId" in resp

    def test_get_commitment_purchase_analysis(self, ce):
        """GetCommitmentPurchaseAnalysis returns a response for a valid analysis ID."""
        import uuid

        resp = ce.get_commitment_purchase_analysis(AnalysisId=str(uuid.uuid4()))
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_cost_allocation_tag_backfill(self, ce):
        """StartCostAllocationTagBackfill returns BackfillRequest."""
        resp = ce.start_cost_allocation_tag_backfill(BackfillFrom="2024-01-01T00:00:00+00:00")
        assert "BackfillRequest" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_cost_allocation_tags_status(self, ce):
        """UpdateCostAllocationTagsStatus returns Errors list."""
        resp = ce.update_cost_allocation_tags_status(
            CostAllocationTagsStatus=[{"TagKey": "env", "Status": "Active"}]
        )
        assert "Errors" in resp
        assert isinstance(resp["Errors"], list)

    def test_get_savings_plan_purchase_recommendation_details(self, ce):
        """GetSavingsPlanPurchaseRecommendationDetails returns a response."""
        import uuid

        resp = ce.get_savings_plan_purchase_recommendation_details(
            RecommendationDetailId=str(uuid.uuid4())
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
