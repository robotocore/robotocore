"""CloudWatch Metrics compatibility tests."""

from datetime import UTC, datetime, timedelta

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
                    "Timestamp": datetime.now(UTC),
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
        now = datetime.now(UTC)
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

    def test_put_metric_alarm_with_sns_action(self, cw):
        """Create an alarm with an SNS topic ARN as an action."""
        sns = make_client("sns")
        topic = sns.create_topic(Name="alarm-notifications")
        topic_arn = topic["TopicArn"]

        cw.put_metric_alarm(
            AlarmName="sns-alarm",
            Namespace="SNSAlarmNS",
            MetricName="Errors",
            ComparisonOperator="GreaterThanOrEqualToThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Sum",
            Threshold=1.0,
            AlarmActions=[topic_arn],
            OKActions=[topic_arn],
            InsufficientDataActions=[topic_arn],
            AlarmDescription="Alarm with SNS integration",
        )
        response = cw.describe_alarms(AlarmNames=["sns-alarm"])
        alarm = response["MetricAlarms"][0]
        assert alarm["AlarmName"] == "sns-alarm"
        assert topic_arn in alarm["AlarmActions"]
        assert topic_arn in alarm["OKActions"]
        assert topic_arn in alarm["InsufficientDataActions"]
        assert alarm["AlarmDescription"] == "Alarm with SNS integration"

        # Set to ALARM state to trigger the action
        cw.set_alarm_state(
            AlarmName="sns-alarm",
            StateValue="ALARM",
            StateReason="Testing SNS integration",
        )
        response = cw.describe_alarms(AlarmNames=["sns-alarm"])
        assert response["MetricAlarms"][0]["StateValue"] == "ALARM"

        cw.delete_alarms(AlarmNames=["sns-alarm"])
        sns.delete_topic(TopicArn=topic_arn)

    def test_describe_alarms_multiple(self, cw):
        """Create multiple alarms and list/filter them."""
        alarm_names = ["multi-alarm-1", "multi-alarm-2", "multi-alarm-3"]
        for name in alarm_names:
            cw.put_metric_alarm(
                AlarmName=name,
                Namespace="MultiNS",
                MetricName="Load",
                ComparisonOperator="GreaterThanThreshold",
                EvaluationPeriods=1,
                Period=60,
                Statistic="Average",
                Threshold=80.0,
            )

        # Describe all by name
        response = cw.describe_alarms(AlarmNames=alarm_names)
        returned_names = {a["AlarmName"] for a in response["MetricAlarms"]}
        assert returned_names == set(alarm_names)

        # Describe with state filter only (no AlarmNames)
        # Set one to ALARM so we can filter
        cw.set_alarm_state(
            AlarmName="multi-alarm-1",
            StateValue="ALARM",
            StateReason="Testing state filter",
        )
        response = cw.describe_alarms(StateValue="ALARM")
        alarm_in_state = [a["AlarmName"] for a in response["MetricAlarms"]]
        assert "multi-alarm-1" in alarm_in_state

        cw.delete_alarms(AlarmNames=alarm_names)

    def test_set_alarm_state_transitions(self, cw):
        """Test transitioning alarm through multiple states."""
        cw.put_metric_alarm(
            AlarmName="transition-alarm",
            Namespace="TransNS",
            MetricName="Health",
            ComparisonOperator="LessThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Minimum",
            Threshold=1.0,
        )

        for state in ["ALARM", "OK", "INSUFFICIENT_DATA", "ALARM"]:
            cw.set_alarm_state(
                AlarmName="transition-alarm",
                StateValue=state,
                StateReason=f"Transitioning to {state}",
            )
            response = cw.describe_alarms(AlarmNames=["transition-alarm"])
            assert response["MetricAlarms"][0]["StateValue"] == state
            assert response["MetricAlarms"][0]["StateReason"] == f"Transitioning to {state}"

        cw.delete_alarms(AlarmNames=["transition-alarm"])

    def test_put_metric_data_with_dimensions(self, cw):
        """Put metric data with dimensions and verify listing."""
        now = datetime.now(UTC)
        cw.put_metric_data(
            Namespace="DimTest",
            MetricData=[
                {
                    "MetricName": "RequestCount",
                    "Dimensions": [
                        {"Name": "Service", "Value": "WebApp"},
                        {"Name": "Environment", "Value": "Production"},
                    ],
                    "Value": 100.0,
                    "Unit": "Count",
                    "Timestamp": now,
                },
                {
                    "MetricName": "RequestCount",
                    "Dimensions": [
                        {"Name": "Service", "Value": "API"},
                        {"Name": "Environment", "Value": "Production"},
                    ],
                    "Value": 200.0,
                    "Unit": "Count",
                    "Timestamp": now,
                },
            ],
        )

        # List metrics filtered by dimension
        response = cw.list_metrics(
            Namespace="DimTest",
            MetricName="RequestCount",
            Dimensions=[{"Name": "Service", "Value": "WebApp"}],
        )
        assert len(response["Metrics"]) >= 1
        metric = response["Metrics"][0]
        dim_names = [d["Name"] for d in metric["Dimensions"]]
        assert "Service" in dim_names

    def test_get_metric_statistics_with_period(self, cw):
        """Test get_metric_statistics with various period and statistic combos."""
        now = datetime.now(UTC)
        # Put multiple data points
        cw.put_metric_data(
            Namespace="PeriodTest",
            MetricData=[
                {"MetricName": "Latency", "Value": 10.0, "Unit": "Milliseconds", "Timestamp": now},
                {"MetricName": "Latency", "Value": 20.0, "Unit": "Milliseconds", "Timestamp": now},
                {"MetricName": "Latency", "Value": 30.0, "Unit": "Milliseconds", "Timestamp": now},
                {"MetricName": "Latency", "Value": 40.0, "Unit": "Milliseconds", "Timestamp": now},
            ],
        )

        # Test with 60-second period
        response_60 = cw.get_metric_statistics(
            Namespace="PeriodTest",
            MetricName="Latency",
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
            Period=60,
            Statistics=["Average", "Sum", "Minimum", "Maximum", "SampleCount"],
        )
        assert "Datapoints" in response_60

        # Test with 300-second period
        response_300 = cw.get_metric_statistics(
            Namespace="PeriodTest",
            MetricName="Latency",
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
            Period=300,
            Statistics=["Average", "Sum"],
        )
        assert "Datapoints" in response_300

        # If we got datapoints, verify statistic values make sense
        if response_60["Datapoints"]:
            dp = response_60["Datapoints"][0]
            assert dp["Minimum"] <= dp["Average"] <= dp["Maximum"]
            assert dp["SampleCount"] >= 1

    def test_delete_alarms(self, cw):
        """Create alarms and verify they are deleted."""
        names = ["del-alarm-1", "del-alarm-2"]
        for name in names:
            cw.put_metric_alarm(
                AlarmName=name,
                Namespace="DelNS",
                MetricName="M",
                ComparisonOperator="GreaterThanThreshold",
                EvaluationPeriods=1,
                Period=60,
                Statistic="Average",
                Threshold=50.0,
            )

        # Verify they exist
        response = cw.describe_alarms(AlarmNames=names)
        assert len(response["MetricAlarms"]) == 2

        # Delete one
        cw.delete_alarms(AlarmNames=["del-alarm-1"])
        response = cw.describe_alarms(AlarmNames=names)
        remaining = [a["AlarmName"] for a in response["MetricAlarms"]]
        assert "del-alarm-1" not in remaining
        assert "del-alarm-2" in remaining

        # Delete the other
        cw.delete_alarms(AlarmNames=["del-alarm-2"])
        response = cw.describe_alarms(AlarmNames=names)
        assert len(response["MetricAlarms"]) == 0

        # Deleting non-existent alarms should not error
        response = cw.delete_alarms(AlarmNames=["nonexistent-alarm"])
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestCloudWatchTagging:
    def test_tag_alarm(self, cw):
        """Tag an alarm and list tags."""
        cw.put_metric_alarm(
            AlarmName="tag-test-alarm",
            Namespace="TagNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        alarms = cw.describe_alarms(AlarmNames=["tag-test-alarm"])
        arn = alarms["MetricAlarms"][0]["AlarmArn"]

        cw.tag_resource(ResourceARN=arn, Tags=[
            {"Key": "env", "Value": "test"},
            {"Key": "team", "Value": "platform"},
        ])
        tags = cw.list_tags_for_resource(ResourceARN=arn)
        tag_map = {t["Key"]: t["Value"] for t in tags["Tags"]}
        assert tag_map["env"] == "test"
        assert tag_map["team"] == "platform"

        cw.delete_alarms(AlarmNames=["tag-test-alarm"])

    def test_untag_alarm(self, cw):
        """Remove a tag from an alarm."""
        cw.put_metric_alarm(
            AlarmName="untag-alarm",
            Namespace="UntagNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        alarms = cw.describe_alarms(AlarmNames=["untag-alarm"])
        arn = alarms["MetricAlarms"][0]["AlarmArn"]

        cw.tag_resource(ResourceARN=arn, Tags=[
            {"Key": "k1", "Value": "v1"},
            {"Key": "k2", "Value": "v2"},
        ])
        cw.untag_resource(ResourceARN=arn, TagKeys=["k1"])
        tags = cw.list_tags_for_resource(ResourceARN=arn)
        tag_map = {t["Key"]: t["Value"] for t in tags["Tags"]}
        assert "k1" not in tag_map
        assert tag_map["k2"] == "v2"

        cw.delete_alarms(AlarmNames=["untag-alarm"])


class TestCloudWatchDashboards:
    def test_put_and_get_dashboard(self, cw):
        """Create and retrieve a dashboard."""
        import json

        body = json.dumps({
            "widgets": [
                {
                    "type": "metric",
                    "x": 0, "y": 0, "width": 12, "height": 6,
                    "properties": {
                        "metrics": [["TestNS", "TestMetric"]],
                        "period": 300,
                    },
                }
            ]
        })
        cw.put_dashboard(DashboardName="test-dash", DashboardBody=body)
        response = cw.get_dashboard(DashboardName="test-dash")
        assert response["DashboardName"] == "test-dash"
        assert "DashboardBody" in response
        assert "DashboardArn" in response

        cw.delete_dashboards(DashboardNames=["test-dash"])

    def test_list_dashboards(self, cw):
        """List dashboards."""
        import json

        body = json.dumps({"widgets": []})
        cw.put_dashboard(DashboardName="list-dash-1", DashboardBody=body)
        cw.put_dashboard(DashboardName="list-dash-2", DashboardBody=body)

        response = cw.list_dashboards()
        names = [d["DashboardName"] for d in response["DashboardEntries"]]
        assert "list-dash-1" in names
        assert "list-dash-2" in names

        cw.delete_dashboards(DashboardNames=["list-dash-1", "list-dash-2"])

    def test_delete_dashboard(self, cw):
        """Delete a dashboard."""
        import json

        body = json.dumps({"widgets": []})
        cw.put_dashboard(DashboardName="del-dash", DashboardBody=body)
        cw.delete_dashboards(DashboardNames=["del-dash"])

        response = cw.list_dashboards(DashboardNamePrefix="del-dash")
        names = [d["DashboardName"] for d in response["DashboardEntries"]]
        assert "del-dash" not in names


class TestCloudWatchAlarmHistory:
    def test_describe_alarm_history(self, cw):
        """Describe alarm history for an alarm."""
        cw.put_metric_alarm(
            AlarmName="history-alarm",
            Namespace="HistNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        cw.set_alarm_state(
            AlarmName="history-alarm",
            StateValue="ALARM",
            StateReason="Test transition",
        )
        response = cw.describe_alarm_history(AlarmName="history-alarm")
        assert "AlarmHistoryItems" in response

        cw.delete_alarms(AlarmNames=["history-alarm"])


class TestCloudWatchAlarmActions:
    def test_enable_disable_alarm_actions(self, cw):
        """Enable and disable alarm actions."""
        cw.put_metric_alarm(
            AlarmName="actions-alarm",
            Namespace="ActionsNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        cw.disable_alarm_actions(AlarmNames=["actions-alarm"])
        response = cw.describe_alarms(AlarmNames=["actions-alarm"])
        assert response["MetricAlarms"][0]["ActionsEnabled"] is False

        cw.enable_alarm_actions(AlarmNames=["actions-alarm"])
        response = cw.describe_alarms(AlarmNames=["actions-alarm"])
        assert response["MetricAlarms"][0]["ActionsEnabled"] is True

        cw.delete_alarms(AlarmNames=["actions-alarm"])


class TestCloudWatchDescribeAlarmsForMetric:
    def test_describe_alarms_for_metric(self, cw):
        """Find alarms associated with a specific metric."""
        cw.put_metric_alarm(
            AlarmName="metric-alarm-1",
            Namespace="MetricAlarmNS",
            MetricName="TargetMetric",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        response = cw.describe_alarms_for_metric(
            Namespace="MetricAlarmNS",
            MetricName="TargetMetric",
        )
        assert "MetricAlarms" in response
        names = [a["AlarmName"] for a in response["MetricAlarms"]]
        assert "metric-alarm-1" in names

        cw.delete_alarms(AlarmNames=["metric-alarm-1"])
