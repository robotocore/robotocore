"""CloudWatch Metrics compatibility tests."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from tests.compatibility.conftest import make_client


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


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


class TestCloudWatchAlarmExtended:
    def test_put_metric_alarm_multiple_dimensions(self, cw):
        """Create alarm with multiple dimensions."""
        alarm_name = _unique("dim-alarm")
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="DimAlarmNS",
            MetricName="Latency",
            Dimensions=[
                {"Name": "Service", "Value": "WebApp"},
                {"Name": "Region", "Value": "us-east-1"},
                {"Name": "Stage", "Value": "prod"},
            ],
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=100.0,
        )
        resp = cw.describe_alarms(AlarmNames=[alarm_name])
        alarm = resp["MetricAlarms"][0]
        dim_names = {d["Name"] for d in alarm["Dimensions"]}
        assert dim_names == {"Service", "Region", "Stage"}
        cw.delete_alarms(AlarmNames=[alarm_name])

    def test_describe_alarms_state_filter(self, cw):
        """Describe alarms filtering by state value."""
        a1 = _unique("sf-alarm1")
        a2 = _unique("sf-alarm2")
        for name in [a1, a2]:
            cw.put_metric_alarm(
                AlarmName=name,
                Namespace="SfNS",
                MetricName="M",
                ComparisonOperator="GreaterThanThreshold",
                EvaluationPeriods=1,
                Period=60,
                Statistic="Average",
                Threshold=50.0,
            )
        cw.set_alarm_state(AlarmName=a1, StateValue="ALARM", StateReason="test")
        cw.set_alarm_state(AlarmName=a2, StateValue="OK", StateReason="test")

        resp = cw.describe_alarms(StateValue="ALARM")
        names = [a["AlarmName"] for a in resp["MetricAlarms"]]
        assert a1 in names
        assert a2 not in names

        resp = cw.describe_alarms(StateValue="OK")
        names = [a["AlarmName"] for a in resp["MetricAlarms"]]
        assert a2 in names

        cw.delete_alarms(AlarmNames=[a1, a2])

    def test_describe_alarms_for_metric(self, cw):
        """Describe alarms for a specific metric."""
        alarm_name = _unique("fm-alarm")
        ns = _unique("FmNS")
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace=ns,
            MetricName="TargetMetric",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=80.0,
        )
        resp = cw.describe_alarms_for_metric(
            Namespace=ns,
            MetricName="TargetMetric",
        )
        names = [a["AlarmName"] for a in resp["MetricAlarms"]]
        assert alarm_name in names
        cw.delete_alarms(AlarmNames=[alarm_name])

    def test_enable_disable_alarm_actions(self, cw):
        """Enable and disable alarm actions."""
        alarm_name = _unique("act-alarm")
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="ActNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        # Disable
        cw.disable_alarm_actions(AlarmNames=[alarm_name])
        resp = cw.describe_alarms(AlarmNames=[alarm_name])
        assert resp["MetricAlarms"][0]["ActionsEnabled"] is False

        # Enable
        cw.enable_alarm_actions(AlarmNames=[alarm_name])
        resp = cw.describe_alarms(AlarmNames=[alarm_name])
        assert resp["MetricAlarms"][0]["ActionsEnabled"] is True

        cw.delete_alarms(AlarmNames=[alarm_name])

    def test_list_metrics_namespace_filter(self, cw):
        """List metrics filtered by namespace."""
        ns = _unique("NsFilt")
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[{"MetricName": "UniqueM", "Value": 1.0, "Unit": "Count"}],
        )
        cw.put_metric_data(
            Namespace="OtherNS",
            MetricData=[{"MetricName": "OtherM", "Value": 1.0, "Unit": "Count"}],
        )
        resp = cw.list_metrics(Namespace=ns)
        for m in resp["Metrics"]:
            assert m["Namespace"] == ns

    def test_get_metric_statistics_different_periods(self, cw):
        """Test get_metric_statistics with 60s vs 300s periods."""
        ns = _unique("PrdNS")
        now = datetime.now(UTC)
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {"MetricName": "Req", "Value": 10.0, "Unit": "Count", "Timestamp": now},
                {"MetricName": "Req", "Value": 20.0, "Unit": "Count", "Timestamp": now},
            ],
        )
        for period in [60, 300]:
            resp = cw.get_metric_statistics(
                Namespace=ns,
                MetricName="Req",
                StartTime=now - timedelta(minutes=5),
                EndTime=now + timedelta(minutes=5),
                Period=period,
                Statistics=["Sum", "Average"],
            )
            assert "Datapoints" in resp


class TestCloudWatchDashboard:
    def test_put_get_delete_dashboard(self, cw):
        """Full dashboard lifecycle."""
        name = _unique("dash")
        body = '{"widgets":[{"type":"text","properties":{"markdown":"Hello"}}]}'
        cw.put_dashboard(DashboardName=name, DashboardBody=body)

        resp = cw.get_dashboard(DashboardName=name)
        assert resp["DashboardName"] == name
        assert "DashboardBody" in resp

        list_resp = cw.list_dashboards()
        names = [d["DashboardName"] for d in list_resp["DashboardEntries"]]
        assert name in names

        cw.delete_dashboards(DashboardNames=[name])
        list_resp = cw.list_dashboards()
        names = [d["DashboardName"] for d in list_resp["DashboardEntries"]]
        assert name not in names


class TestCloudWatchTagging:
    def test_tag_untag_alarm(self, cw):
        """Tag and untag a CloudWatch alarm."""
        alarm_name = _unique("tag-alarm")
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="TagNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        # Get alarm ARN
        resp = cw.describe_alarms(AlarmNames=[alarm_name])
        alarm_arn = resp["MetricAlarms"][0]["AlarmArn"]

        cw.tag_resource(
            ResourceARN=alarm_arn,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "backend"},
            ],
        )
        tag_resp = cw.list_tags_for_resource(ResourceARN=alarm_arn)
        tag_keys = {t["Key"] for t in tag_resp["Tags"]}
        assert "env" in tag_keys
        assert "team" in tag_keys

        cw.untag_resource(ResourceARN=alarm_arn, TagKeys=["team"])
        tag_resp = cw.list_tags_for_resource(ResourceARN=alarm_arn)
        tag_keys = {t["Key"] for t in tag_resp["Tags"]}
        assert "env" in tag_keys
        assert "team" not in tag_keys

        cw.delete_alarms(AlarmNames=[alarm_name])


class TestCloudWatchAlarmHistory:
    def test_describe_alarm_history(self, cw):
        """Describe alarm history after state changes."""
        alarm_name = _unique("hist-alarm")
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="HistNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        cw.set_alarm_state(
            AlarmName=alarm_name, StateValue="ALARM", StateReason="trigger history"
        )
        resp = cw.describe_alarm_history(AlarmName=alarm_name)
        assert "AlarmHistoryItems" in resp
        cw.delete_alarms(AlarmNames=[alarm_name])


class TestCloudWatchGetMetricData:
    def test_get_metric_data_basic(self, cw):
        """Basic get_metric_data call."""
        ns = _unique("GmdNS")
        now = datetime.now(UTC)
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {"MetricName": "Cpu", "Value": 60.0, "Unit": "Percent", "Timestamp": now},
            ],
        )
        resp = cw.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "m1",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": ns,
                            "MetricName": "Cpu",
                        },
                        "Period": 60,
                        "Stat": "Average",
                    },
                    "ReturnData": True,
                }
            ],
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
        )
        assert "MetricDataResults" in resp
        assert len(resp["MetricDataResults"]) >= 1
        assert resp["MetricDataResults"][0]["Id"] == "m1"

    def test_get_metric_data_with_math(self, cw):
        """get_metric_data with a math expression."""
        ns = _unique("MathNS")
        now = datetime.now(UTC)
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {"MetricName": "Req", "Value": 100.0, "Unit": "Count", "Timestamp": now},
                {"MetricName": "Err", "Value": 5.0, "Unit": "Count", "Timestamp": now},
            ],
        )
        resp = cw.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "requests",
                    "MetricStat": {
                        "Metric": {"Namespace": ns, "MetricName": "Req"},
                        "Period": 60,
                        "Stat": "Sum",
                    },
                    "ReturnData": False,
                },
                {
                    "Id": "errors",
                    "MetricStat": {
                        "Metric": {"Namespace": ns, "MetricName": "Err"},
                        "Period": 60,
                        "Stat": "Sum",
                    },
                    "ReturnData": False,
                },
                {
                    "Id": "error_rate",
                    "Expression": "errors / requests * 100",
                    "ReturnData": True,
                },
            ],
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
        )
        assert "MetricDataResults" in resp
        # The math expression result should be returned
        result_ids = [r["Id"] for r in resp["MetricDataResults"]]
        assert "error_rate" in result_ids


class TestCloudWatchAlarmUpdate:
    def test_update_alarm_threshold(self, cw):
        """Update an existing alarm by re-putting with same name."""
        alarm_name = _unique("upd-alarm")
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="UpdNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        # Update threshold
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="UpdNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=90.0,
        )
        resp = cw.describe_alarms(AlarmNames=[alarm_name])
        assert resp["MetricAlarms"][0]["Threshold"] == 90.0
        cw.delete_alarms(AlarmNames=[alarm_name])

    def test_alarm_description(self, cw):
        """Verify alarm description is stored and returned."""
        alarm_name = _unique("desc-alarm")
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="DescNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
            AlarmDescription="This is a test alarm",
        )
        resp = cw.describe_alarms(AlarmNames=[alarm_name])
        assert resp["MetricAlarms"][0]["AlarmDescription"] == "This is a test alarm"
        cw.delete_alarms(AlarmNames=[alarm_name])


class TestCloudWatchMetricDataBatch:
    def test_put_metric_data_batch(self, cw):
        """Put multiple metrics in a single call."""
        ns = _unique("BatchNS")
        now = datetime.now(UTC)
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {"MetricName": "M1", "Value": 1.0, "Unit": "Count", "Timestamp": now},
                {"MetricName": "M2", "Value": 2.0, "Unit": "Count", "Timestamp": now},
                {"MetricName": "M3", "Value": 3.0, "Unit": "Count", "Timestamp": now},
            ],
        )
        resp = cw.list_metrics(Namespace=ns)
        names = {m["MetricName"] for m in resp["Metrics"]}
        assert "M1" in names
        assert "M2" in names
        assert "M3" in names

    def test_list_metrics_metric_name_filter(self, cw):
        """List metrics filtered by metric name."""
        ns = _unique("MnfNS")
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {"MetricName": "Alpha", "Value": 1.0, "Unit": "Count"},
                {"MetricName": "Beta", "Value": 2.0, "Unit": "Count"},
            ],
        )
        resp = cw.list_metrics(Namespace=ns, MetricName="Alpha")
        names = [m["MetricName"] for m in resp["Metrics"]]
        assert "Alpha" in names
        assert "Beta" not in names
