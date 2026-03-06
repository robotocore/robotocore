"""CloudWatch Metrics compatibility tests."""

from datetime import datetime, timedelta, timezone

import pytest
from tests.compatibility.conftest import make_client


@pytest.fixture
def cw():
    return make_client("cloudwatch")


class TestCloudWatchOperations:
    def test_put_metric_data(self, cw):
        response = cw.put_metric_data(
            Namespace="TestNamespace",
            MetricData=[
                {
                    "MetricName": "TestMetric",
                    "Value": 42.0,
                    "Unit": "Count",
                    "Timestamp": datetime.now(timezone.utc),
                }
            ],
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_metrics(self, cw):
        cw.put_metric_data(
            Namespace="ListTest",
            MetricData=[{"MetricName": "Metric1", "Value": 1.0, "Unit": "None"}],
        )
        response = cw.list_metrics(Namespace="ListTest")
        names = [m["MetricName"] for m in response["Metrics"]]
        assert "Metric1" in names

    def test_put_metric_alarm(self, cw):
        cw.put_metric_alarm(
            AlarmName="test-alarm",
            Namespace="TestNS",
            MetricName="TestMetric",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=90.0,
        )
        response = cw.describe_alarms(AlarmNames=["test-alarm"])
        assert len(response["MetricAlarms"]) == 1
        assert response["MetricAlarms"][0]["AlarmName"] == "test-alarm"
        cw.delete_alarms(AlarmNames=["test-alarm"])

    def test_describe_alarms(self, cw):
        cw.put_metric_alarm(
            AlarmName="desc-alarm",
            Namespace="DescNS",
            MetricName="M1",
            ComparisonOperator="LessThanThreshold",
            EvaluationPeriods=1,
            Period=300,
            Statistic="Sum",
            Threshold=10.0,
        )
        response = cw.describe_alarms()
        names = [a["AlarmName"] for a in response["MetricAlarms"]]
        assert "desc-alarm" in names
        cw.delete_alarms(AlarmNames=["desc-alarm"])

    def test_get_metric_statistics(self, cw):
        now = datetime.now(timezone.utc)
        cw.put_metric_data(
            Namespace="StatsTest",
            MetricData=[
                {"MetricName": "CPU", "Value": 50.0, "Unit": "Percent", "Timestamp": now},
                {"MetricName": "CPU", "Value": 70.0, "Unit": "Percent", "Timestamp": now},
            ],
        )
        response = cw.get_metric_statistics(
            Namespace="StatsTest",
            MetricName="CPU",
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
            Period=300,
            Statistics=["Average", "Sum"],
        )
        assert "Datapoints" in response

    def test_set_alarm_state(self, cw):
        cw.put_metric_alarm(
            AlarmName="state-alarm",
            Namespace="StateNS",
            MetricName="M1",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        cw.set_alarm_state(
            AlarmName="state-alarm",
            StateValue="ALARM",
            StateReason="Testing",
        )
        response = cw.describe_alarms(AlarmNames=["state-alarm"])
        assert response["MetricAlarms"][0]["StateValue"] == "ALARM"
        cw.delete_alarms(AlarmNames=["state-alarm"])
