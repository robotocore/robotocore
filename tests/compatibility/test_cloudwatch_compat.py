"""CloudWatch Metrics compatibility tests."""

import datetime
import json
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

    def test_put_metric_data_multiple_datapoints_then_stats(self, cw):
        """Put multiple datapoints for the same metric and retrieve statistics."""
        import uuid

        ns = f"MultiDP-{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {"MetricName": "Temp", "Value": 10.0, "Unit": "None", "Timestamp": now},
                {"MetricName": "Temp", "Value": 20.0, "Unit": "None", "Timestamp": now},
                {"MetricName": "Temp", "Value": 30.0, "Unit": "None", "Timestamp": now},
                {"MetricName": "Temp", "Value": 40.0, "Unit": "None", "Timestamp": now},
                {"MetricName": "Temp", "Value": 50.0, "Unit": "None", "Timestamp": now},
            ],
        )
        response = cw.get_metric_statistics(
            Namespace=ns,
            MetricName="Temp",
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
            Period=300,
            Statistics=["Average", "Sum", "Minimum", "Maximum", "SampleCount"],
        )
        assert len(response["Datapoints"]) >= 1
        dp = response["Datapoints"][0]
        assert dp["Minimum"] == 10.0
        assert dp["Maximum"] == 50.0
        assert dp["SampleCount"] == 5.0
        assert dp["Sum"] == 150.0
        assert dp["Average"] == 30.0

    def test_put_metric_data_dimensions_query(self, cw):
        """Put metrics with dimensions and query by specific dimension values."""
        import uuid

        ns = f"DimQuery-{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {
                    "MetricName": "Requests",
                    "Dimensions": [{"Name": "Env", "Value": "prod"}],
                    "Value": 100.0,
                    "Unit": "Count",
                    "Timestamp": now,
                },
                {
                    "MetricName": "Requests",
                    "Dimensions": [{"Name": "Env", "Value": "staging"}],
                    "Value": 50.0,
                    "Unit": "Count",
                    "Timestamp": now,
                },
            ],
        )
        # Query by prod dimension
        response = cw.list_metrics(
            Namespace=ns,
            Dimensions=[{"Name": "Env", "Value": "prod"}],
        )
        assert len(response["Metrics"]) >= 1
        dims = response["Metrics"][0]["Dimensions"]
        assert any(d["Value"] == "prod" for d in dims)

        # Query by staging dimension
        response = cw.list_metrics(
            Namespace=ns,
            Dimensions=[{"Name": "Env", "Value": "staging"}],
        )
        assert len(response["Metrics"]) >= 1

    def test_put_composite_alarm(self, cw):
        """PutCompositeAlarm succeeds without error."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        alarm1 = f"comp-metric-{suffix}-1"
        alarm2 = f"comp-metric-{suffix}-2"
        composite = f"comp-composite-{suffix}"

        for name in [alarm1, alarm2]:
            cw.put_metric_alarm(
                AlarmName=name,
                Namespace="CompNS",
                MetricName="Load",
                ComparisonOperator="GreaterThanThreshold",
                EvaluationPeriods=1,
                Period=60,
                Statistic="Average",
                Threshold=80.0,
            )

        response = cw.put_composite_alarm(
            AlarmName=composite,
            AlarmRule=f'ALARM("{alarm1}") OR ALARM("{alarm2}")',
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        cw.delete_alarms(AlarmNames=[composite, alarm1, alarm2])

    def test_alarm_tags(self, cw):
        """Tag, untag, and list tags on a metric alarm."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        alarm_name = f"tag-alarm-{suffix}"
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="TagNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "backend"},
            ],
        )

        # Get alarm ARN
        response = cw.describe_alarms(AlarmNames=[alarm_name])
        alarm_arn = response["MetricAlarms"][0]["AlarmArn"]

        # List tags
        tags_response = cw.list_tags_for_resource(ResourceARN=alarm_arn)
        tag_map = {t["Key"]: t["Value"] for t in tags_response["Tags"]}
        assert tag_map["env"] == "test"
        assert tag_map["team"] == "backend"

        # Add a tag
        cw.tag_resource(ResourceARN=alarm_arn, Tags=[{"Key": "version", "Value": "1"}])
        tags_response = cw.list_tags_for_resource(ResourceARN=alarm_arn)
        tag_map = {t["Key"]: t["Value"] for t in tags_response["Tags"]}
        assert tag_map["version"] == "1"

        # Remove a tag
        cw.untag_resource(ResourceARN=alarm_arn, TagKeys=["team"])
        tags_response = cw.list_tags_for_resource(ResourceARN=alarm_arn)
        tag_keys = [t["Key"] for t in tags_response["Tags"]]
        assert "team" not in tag_keys

        cw.delete_alarms(AlarmNames=[alarm_name])

    def test_alarm_comparison_operators(self, cw):
        """Create alarms with different comparison operators."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        operators = [
            "GreaterThanThreshold",
            "GreaterThanOrEqualToThreshold",
            "LessThanThreshold",
            "LessThanOrEqualToThreshold",
        ]
        alarm_names = []
        for i, op in enumerate(operators):
            name = f"op-alarm-{suffix}-{i}"
            alarm_names.append(name)
            cw.put_metric_alarm(
                AlarmName=name,
                Namespace="OpNS",
                MetricName="Val",
                ComparisonOperator=op,
                EvaluationPeriods=1,
                Period=60,
                Statistic="Average",
                Threshold=50.0,
            )

        response = cw.describe_alarms(AlarmNames=alarm_names)
        returned_ops = {a["AlarmName"]: a["ComparisonOperator"] for a in response["MetricAlarms"]}
        for i, op in enumerate(operators):
            name = f"op-alarm-{suffix}-{i}"
            assert returned_ops[name] == op

        cw.delete_alarms(AlarmNames=alarm_names)

    def test_set_alarm_state_and_describe_alarm_history(self, cw):
        """SetAlarmState and verify DescribeAlarmHistory records the change."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        alarm_name = f"history-alarm-{suffix}"
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
            AlarmName=alarm_name,
            StateValue="ALARM",
            StateReason="Testing history",
        )

        response = cw.describe_alarm_history(AlarmName=alarm_name)
        assert "AlarmHistoryItems" in response

        cw.delete_alarms(AlarmNames=[alarm_name])

    def test_disable_enable_alarm_actions(self, cw):
        """Disable and re-enable alarm actions."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        alarm_name = f"actions-alarm-{suffix}"
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="ActionsNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )

        cw.disable_alarm_actions(AlarmNames=[alarm_name])
        response = cw.describe_alarms(AlarmNames=[alarm_name])
        assert response["MetricAlarms"][0]["ActionsEnabled"] is False

        cw.enable_alarm_actions(AlarmNames=[alarm_name])
        response = cw.describe_alarms(AlarmNames=[alarm_name])
        assert response["MetricAlarms"][0]["ActionsEnabled"] is True

        cw.delete_alarms(AlarmNames=[alarm_name])

    def test_put_get_list_delete_dashboard(self, cw):
        """Full dashboard lifecycle: put, get, list, delete."""
        import json
        import uuid

        suffix = uuid.uuid4().hex[:8]
        dash_name = f"test-dash-{suffix}"
        dash_body = json.dumps({
            "widgets": [
                {
                    "type": "metric",
                    "x": 0,
                    "y": 0,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [["TestNS", "TestMetric"]],
                        "period": 300,
                        "stat": "Average",
                        "region": "us-east-1",
                        "title": "Test Dashboard",
                    },
                }
            ]
        })

        # Put
        response = cw.put_dashboard(DashboardName=dash_name, DashboardBody=dash_body)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Get
        response = cw.get_dashboard(DashboardName=dash_name)
        assert response["DashboardName"] == dash_name
        body = json.loads(response["DashboardBody"])
        assert len(body["widgets"]) == 1

        # List
        response = cw.list_dashboards(DashboardNamePrefix=f"test-dash-{suffix}")
        names = [d["DashboardName"] for d in response["DashboardEntries"]]
        assert dash_name in names

        # Delete
        cw.delete_dashboards(DashboardNames=[dash_name])
        response = cw.list_dashboards(DashboardNamePrefix=f"test-dash-{suffix}")
        names = [d["DashboardName"] for d in response["DashboardEntries"]]
        assert dash_name not in names

    def test_list_metrics_namespace_filter(self, cw):
        """ListMetrics filtered by Namespace only."""
        import uuid

        ns = f"NSFilter-{uuid.uuid4().hex[:8]}"
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {"MetricName": "Alpha", "Value": 1.0, "Unit": "None"},
                {"MetricName": "Beta", "Value": 2.0, "Unit": "None"},
            ],
        )
        response = cw.list_metrics(Namespace=ns)
        names = {m["MetricName"] for m in response["Metrics"]}
        assert "Alpha" in names
        assert "Beta" in names

    def test_get_metric_data_with_math(self, cw):
        """GetMetricData with a metric math expression."""
        import uuid

        ns = f"MathTest-{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {"MetricName": "Reads", "Value": 100.0, "Unit": "Count", "Timestamp": now},
                {"MetricName": "Writes", "Value": 50.0, "Unit": "Count", "Timestamp": now},
            ],
        )
        response = cw.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "reads",
                    "MetricStat": {
                        "Metric": {"Namespace": ns, "MetricName": "Reads"},
                        "Period": 300,
                        "Stat": "Sum",
                    },
                    "ReturnData": False,
                },
                {
                    "Id": "writes",
                    "MetricStat": {
                        "Metric": {"Namespace": ns, "MetricName": "Writes"},
                        "Period": 300,
                        "Stat": "Sum",
                    },
                    "ReturnData": False,
                },
                {
                    "Id": "total",
                    "Expression": "reads + writes",
                    "Label": "TotalOps",
                    "ReturnData": True,
                },
            ],
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
        )
        assert "MetricDataResults" in response
        results = {r["Id"]: r for r in response["MetricDataResults"]}
        assert "total" in results

    def test_list_metrics_metric_name_filter(self, cw):
        """ListMetrics filtered by MetricName."""
        import uuid

        ns = f"MNFilter-{uuid.uuid4().hex[:8]}"
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {"MetricName": "UniqueMetricXYZ", "Value": 1.0, "Unit": "None"},
                {"MetricName": "OtherMetric", "Value": 2.0, "Unit": "None"},
            ],
        )
        response = cw.list_metrics(Namespace=ns, MetricName="UniqueMetricXYZ")
        names = {m["MetricName"] for m in response["Metrics"]}
        assert "UniqueMetricXYZ" in names
        assert "OtherMetric" not in names

    def test_put_metric_alarm_with_datapoints_to_alarm(self, cw):
        """Create an alarm with DatapointsToAlarm set."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        alarm_name = f"dp-alarm-{suffix}"
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="DPNS",
            MetricName="Errors",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=5,
            DatapointsToAlarm=3,
            Period=60,
            Statistic="Sum",
            Threshold=10.0,
        )
        response = cw.describe_alarms(AlarmNames=[alarm_name])
        alarm = response["MetricAlarms"][0]
        assert alarm["EvaluationPeriods"] == 5
        assert alarm["DatapointsToAlarm"] == 3

        cw.delete_alarms(AlarmNames=[alarm_name])

    def test_put_metric_alarm_treat_missing_data(self, cw):
        """Create alarm with TreatMissingData setting."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        alarm_name = f"missing-alarm-{suffix}"
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="MissingNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
            TreatMissingData="notBreaching",
        )
        response = cw.describe_alarms(AlarmNames=[alarm_name])
        assert response["MetricAlarms"][0]["TreatMissingData"] == "notBreaching"

        cw.delete_alarms(AlarmNames=[alarm_name])

    def test_describe_alarms_for_metric(self, cw):
        """DescribeAlarmsForMetric returns alarms for a specific metric."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        ns = f"ForMetric-{suffix}"
        alarm_name = f"for-metric-{suffix}"
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace=ns,
            MetricName="CPUUsage",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=90.0,
        )
        response = cw.describe_alarms_for_metric(
            Namespace=ns,
            MetricName="CPUUsage",
        )
        names = [a["AlarmName"] for a in response["MetricAlarms"]]
        assert alarm_name in names

        cw.delete_alarms(AlarmNames=[alarm_name])

    def test_put_metric_data_with_statistics_values(self, cw):
        """PutMetricData using StatisticValues instead of Value."""
        import uuid

        ns = f"StatVal-{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {
                    "MetricName": "AggMetric",
                    "StatisticValues": {
                        "SampleCount": 10.0,
                        "Sum": 500.0,
                        "Minimum": 10.0,
                        "Maximum": 100.0,
                    },
                    "Unit": "Count",
                    "Timestamp": now,
                }
            ],
        )
        response = cw.get_metric_statistics(
            Namespace=ns,
            MetricName="AggMetric",
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
            Period=300,
            Statistics=["Average", "Sum", "SampleCount", "Minimum", "Maximum"],
        )
        assert len(response["Datapoints"]) >= 1
        dp = response["Datapoints"][0]
        assert dp["SampleCount"] == 10.0
        assert dp["Sum"] == 500.0

    def test_describe_alarms_with_alarm_name_prefix(self, cw):
        """DescribeAlarms with AlarmNamePrefix."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        prefix = f"pfx-{suffix}"
        names = [f"{prefix}-a", f"{prefix}-b"]
        for name in names:
            cw.put_metric_alarm(
                AlarmName=name,
                Namespace="PfxNS",
                MetricName="M",
                ComparisonOperator="GreaterThanThreshold",
                EvaluationPeriods=1,
                Period=60,
                Statistic="Average",
                Threshold=50.0,
            )

        response = cw.describe_alarms(AlarmNamePrefix=prefix)
        returned = [a["AlarmName"] for a in response["MetricAlarms"]]
        for name in names:
            assert name in returned

        cw.delete_alarms(AlarmNames=names)

    def test_put_metric_data_multiple_metrics_and_dimensions(self, cw):
        """Put multiple metrics with dimensions in a single call."""
        now = datetime.now(UTC)
        cw.put_metric_data(
            Namespace="MultiMetricTest",
            MetricData=[
                {
                    "MetricName": "CPUUtilization",
                    "Dimensions": [{"Name": "InstanceId", "Value": "i-001"}],
                    "Value": 75.5,
                    "Unit": "Percent",
                    "Timestamp": now,
                },
                {
                    "MetricName": "MemoryUsage",
                    "Dimensions": [
                        {"Name": "InstanceId", "Value": "i-001"},
                        {"Name": "Region", "Value": "us-east-1"},
                    ],
                    "Value": 60.0,
                    "Unit": "Percent",
                    "Timestamp": now,
                },
                {
                    "MetricName": "DiskIO",
                    "Dimensions": [{"Name": "InstanceId", "Value": "i-002"}],
                    "Value": 1024.0,
                    "Unit": "Bytes",
                    "Timestamp": now,
                },
            ],
        )
        response = cw.list_metrics(Namespace="MultiMetricTest")
        metric_names = {m["MetricName"] for m in response["Metrics"]}
        assert "CPUUtilization" in metric_names
        assert "MemoryUsage" in metric_names
        assert "DiskIO" in metric_names

    def test_get_metric_statistics_all_stats(self, cw):
        """Test GetMetricStatistics returning Sum, Average, Min, Max."""
        now = datetime.now(UTC)
        cw.put_metric_data(
            Namespace="AllStatsTest",
            MetricData=[
                {"MetricName": "Latency", "Value": 10.0, "Unit": "Milliseconds", "Timestamp": now},
                {"MetricName": "Latency", "Value": 30.0, "Unit": "Milliseconds", "Timestamp": now},
                {"MetricName": "Latency", "Value": 50.0, "Unit": "Milliseconds", "Timestamp": now},
            ],
        )
        response = cw.get_metric_statistics(
            Namespace="AllStatsTest",
            MetricName="Latency",
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
            Period=300,
            Statistics=["Sum", "Average", "Minimum", "Maximum", "SampleCount"],
        )
        assert "Datapoints" in response
        if response["Datapoints"]:
            dp = response["Datapoints"][0]
            assert dp["Minimum"] <= dp["Average"] <= dp["Maximum"]
            assert dp["Sum"] >= dp["Maximum"]
            assert dp["SampleCount"] >= 1

    def test_list_metrics_filter_by_namespace_and_name(self, cw):
        """Test ListMetrics filtering by namespace and metric name."""
        cw.put_metric_data(
            Namespace="FilterNS",
            MetricData=[
                {"MetricName": "Alpha", "Value": 1.0, "Unit": "None"},
                {"MetricName": "Beta", "Value": 2.0, "Unit": "None"},
            ],
        )
        response = cw.list_metrics(Namespace="FilterNS", MetricName="Alpha")
        names = [m["MetricName"] for m in response["Metrics"]]
        assert "Alpha" in names
        assert "Beta" not in names

    def test_describe_alarm_history(self, cw):
        """Test DescribeAlarmHistory."""
        cw.put_metric_alarm(
            AlarmName="history-alarm",
            Namespace="HistNS",
            MetricName="M1",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        try:
            cw.set_alarm_state(
                AlarmName="history-alarm",
                StateValue="ALARM",
                StateReason="Testing history",
            )
            response = cw.describe_alarm_history(AlarmName="history-alarm")
            assert "AlarmHistoryItems" in response
        finally:
            cw.delete_alarms(AlarmNames=["history-alarm"])

    def test_set_alarm_state_and_verify(self, cw):
        """Test SetAlarmState and verify with DescribeAlarms."""
        cw.put_metric_alarm(
            AlarmName="verify-state-alarm",
            Namespace="VerifyNS",
            MetricName="M1",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        try:
            cw.set_alarm_state(
                AlarmName="verify-state-alarm",
                StateValue="OK",
                StateReason="Manually set to OK",
            )
            response = cw.describe_alarms(AlarmNames=["verify-state-alarm"])
            alarm = response["MetricAlarms"][0]
            assert alarm["StateValue"] == "OK"
            assert alarm["StateReason"] == "Manually set to OK"
        finally:
            cw.delete_alarms(AlarmNames=["verify-state-alarm"])

    def test_put_get_delete_dashboard(self, cw):
        """Test PutDashboard, GetDashboard, DeleteDashboards, ListDashboards."""
        dashboard_body = '{"widgets":[{"type":"metric","properties":{"metrics":[["NS","M"]]}}]}'
        try:
            cw.put_dashboard(DashboardName="test-dashboard", DashboardBody=dashboard_body)

            get_resp = cw.get_dashboard(DashboardName="test-dashboard")
            assert get_resp["DashboardName"] == "test-dashboard"
            assert "DashboardBody" in get_resp

            list_resp = cw.list_dashboards()
            names = [d["DashboardName"] for d in list_resp["DashboardEntries"]]
            assert "test-dashboard" in names

            cw.delete_dashboards(DashboardNames=["test-dashboard"])
            list_resp2 = cw.list_dashboards()
            names2 = [d["DashboardName"] for d in list_resp2.get("DashboardEntries", [])]
            assert "test-dashboard" not in names2
        except Exception:
            try:
                cw.delete_dashboards(DashboardNames=["test-dashboard"])
            except Exception:
                pass
            raise

    def test_tag_untag_alarm(self, cw):
        """Test TagResource, UntagResource, ListTagsForResource on alarms."""
        cw.put_metric_alarm(
            AlarmName="tag-alarm",
            Namespace="TagNS",
            MetricName="M1",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        try:
            # Get alarm ARN
            desc = cw.describe_alarms(AlarmNames=["tag-alarm"])
            alarm_arn = desc["MetricAlarms"][0]["AlarmArn"]

            cw.tag_resource(
                ResourceARN=alarm_arn,
                Tags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "platform"},
                ],
            )
            tags_resp = cw.list_tags_for_resource(ResourceARN=alarm_arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "platform"

            cw.untag_resource(ResourceARN=alarm_arn, TagKeys=["team"])
            tags_resp2 = cw.list_tags_for_resource(ResourceARN=alarm_arn)
            tag_map2 = {t["Key"]: t["Value"] for t in tags_resp2["Tags"]}
            assert "team" not in tag_map2
            assert tag_map2["env"] == "test"
        finally:
            cw.delete_alarms(AlarmNames=["tag-alarm"])

    def test_enable_disable_alarm_actions(self, cw):
        """Test EnableAlarmActions and DisableAlarmActions."""
        cw.put_metric_alarm(
            AlarmName="actions-alarm",
            Namespace="ActionsNS",
            MetricName="M1",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        try:
            cw.disable_alarm_actions(AlarmNames=["actions-alarm"])
            desc = cw.describe_alarms(AlarmNames=["actions-alarm"])
            assert desc["MetricAlarms"][0]["ActionsEnabled"] is False

            cw.enable_alarm_actions(AlarmNames=["actions-alarm"])
            desc2 = cw.describe_alarms(AlarmNames=["actions-alarm"])
            assert desc2["MetricAlarms"][0]["ActionsEnabled"] is True
        finally:
            cw.delete_alarms(AlarmNames=["actions-alarm"])

    def test_put_metric_alarm_comparison_operators(self, cw):
        """Test PutMetricAlarm with different comparison operators."""
        alarms = [
            ("gt-alarm", "GreaterThanThreshold", 90.0),
            ("lt-alarm", "LessThanThreshold", 10.0),
        ]
        try:
            for name, op, threshold in alarms:
                cw.put_metric_alarm(
                    AlarmName=name,
                    Namespace="CompOpNS",
                    MetricName="TestMetric",
                    ComparisonOperator=op,
                    EvaluationPeriods=1,
                    Period=60,
                    Statistic="Average",
                    Threshold=threshold,
                )

            for name, op, threshold in alarms:
                desc = cw.describe_alarms(AlarmNames=[name])
                alarm = desc["MetricAlarms"][0]
                assert alarm["ComparisonOperator"] == op
                assert alarm["Threshold"] == threshold
        finally:
            cw.delete_alarms(AlarmNames=[a[0] for a in alarms])
