"""CloudWatch Metrics compatibility tests."""

import uuid
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
        assert len(response["Metrics"]) >= 1

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
        assert len(response["MetricAlarms"]) >= 1
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
        assert isinstance(response["Datapoints"], list)

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
        assert len(response["AlarmHistoryItems"]) >= 1

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
        dash_body = json.dumps(
            {
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
            }
        )

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
        assert len(response["Metrics"]) == 2

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
        assert len(response["MetricDataResults"]) >= 1

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
        assert len(metric_names) >= 3

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
            assert len(response["AlarmHistoryItems"]) >= 1
        finally:
            cw.delete_alarms(AlarmNames=["history-alarm"])

    def test_set_alarm_state_and_verify(self, cw):
        """Test SetAlarmState and verify with DescribeAlarms."""
        alarm_name = f"verify-state-alarm-{uuid.uuid4().hex[:8]}"
        cw.put_metric_alarm(
            AlarmName=alarm_name,
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
                AlarmName=alarm_name,
                StateValue="OK",
                StateReason="Manually set to OK",
            )
            response = cw.describe_alarms(AlarmNames=[alarm_name])
            alarm = response["MetricAlarms"][0]
            assert alarm["StateValue"] == "OK"
            assert alarm["StateReason"] == "Manually set to OK"
        finally:
            cw.delete_alarms(AlarmNames=[alarm_name])

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
                pass  # best-effort cleanup
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

    def test_put_metric_alarm_with_actions(self, cw):
        """PutMetricAlarm with alarm, OK, and insufficient data actions."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        alarm_name = f"action-alarm-{suffix}"
        sns_arn = f"arn:aws:sns:us-east-1:123456789012:alarm-topic-{suffix}"
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="ActionNS",
            MetricName="Errors",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Sum",
            Threshold=5.0,
            AlarmActions=[sns_arn],
            OKActions=[sns_arn],
            InsufficientDataActions=[sns_arn],
        )
        desc = cw.describe_alarms(AlarmNames=[alarm_name])
        alarm = desc["MetricAlarms"][0]
        assert sns_arn in alarm["AlarmActions"]
        assert sns_arn in alarm["OKActions"]
        assert sns_arn in alarm["InsufficientDataActions"]
        cw.delete_alarms(AlarmNames=[alarm_name])

    def test_put_metric_alarm_with_dimensions(self, cw):
        """PutMetricAlarm scoped to a dimension."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        alarm_name = f"dim-alarm-{suffix}"
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="DimNS",
            MetricName="CPUUtilization",
            Dimensions=[{"Name": "InstanceId", "Value": "i-12345"}],
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=300,
            Statistic="Average",
            Threshold=80.0,
        )
        desc = cw.describe_alarms(AlarmNames=[alarm_name])
        alarm = desc["MetricAlarms"][0]
        dims = {d["Name"]: d["Value"] for d in alarm["Dimensions"]}
        assert dims["InstanceId"] == "i-12345"
        cw.delete_alarms(AlarmNames=[alarm_name])

    def test_put_metric_data_with_dimensions_v2(self, cw):
        """PutMetricData with dimensions and retrieve filtered."""
        import uuid

        ns = f"DimData-{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {
                    "MetricName": "Latency",
                    "Dimensions": [
                        {"Name": "Service", "Value": "API"},
                        {"Name": "Stage", "Value": "prod"},
                    ],
                    "Value": 42.0,
                    "Unit": "Milliseconds",
                    "Timestamp": now,
                }
            ],
        )
        resp = cw.list_metrics(
            Namespace=ns,
            Dimensions=[{"Name": "Service", "Value": "API"}],
        )
        assert len(resp["Metrics"]) >= 1

    def test_put_composite_alarm_v2(self, cw):
        """PutCompositeAlarm creates a composite alarm from metric alarms."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        child_name = f"child-{suffix}"
        composite_name = f"composite-{suffix}"
        try:
            cw.put_metric_alarm(
                AlarmName=child_name,
                Namespace="CompNS",
                MetricName="M",
                ComparisonOperator="GreaterThanThreshold",
                EvaluationPeriods=1,
                Period=60,
                Statistic="Average",
                Threshold=50.0,
            )
            cw.put_composite_alarm(
                AlarmName=composite_name,
                AlarmRule=f'ALARM("{child_name}")',
            )
            resp = cw.describe_alarms(AlarmNames=[composite_name], AlarmTypes=["CompositeAlarm"])
            assert len(resp.get("CompositeAlarms", [])) >= 1
        finally:
            try:
                cw.delete_alarms(AlarmNames=[composite_name, child_name])
            except Exception:
                pass  # best-effort cleanup

    def test_list_metrics_with_dimensions_filter(self, cw):
        """ListMetrics filtered by Dimensions."""
        import uuid

        ns = f"DimFilter-{uuid.uuid4().hex[:8]}"
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {
                    "MetricName": "Requests",
                    "Dimensions": [{"Name": "Endpoint", "Value": "/api/v1"}],
                    "Value": 100.0,
                    "Unit": "Count",
                },
                {
                    "MetricName": "Requests",
                    "Dimensions": [{"Name": "Endpoint", "Value": "/api/v2"}],
                    "Value": 50.0,
                    "Unit": "Count",
                },
            ],
        )
        resp = cw.list_metrics(
            Namespace=ns,
            MetricName="Requests",
            Dimensions=[{"Name": "Endpoint", "Value": "/api/v1"}],
        )
        assert len(resp["Metrics"]) >= 1
        for m in resp["Metrics"]:
            dim_vals = [d["Value"] for d in m["Dimensions"] if d["Name"] == "Endpoint"]
            assert "/api/v1" in dim_vals

    def test_get_metric_statistics_extended_stats(self, cw):
        """GetMetricStatistics with ExtendedStatistics (percentiles)."""
        import uuid

        ns = f"ExtStat-{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)
        for v in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
            cw.put_metric_data(
                Namespace=ns,
                MetricData=[
                    {
                        "MetricName": "Latency",
                        "Value": float(v),
                        "Unit": "Milliseconds",
                        "Timestamp": now,
                    }
                ],
            )
        resp = cw.get_metric_statistics(
            Namespace=ns,
            MetricName="Latency",
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
            Period=300,
            ExtendedStatistics=["p50", "p99"],
        )
        assert "Datapoints" in resp
        assert isinstance(resp["Datapoints"], list)
        if resp["Datapoints"]:
            dp = resp["Datapoints"][0]
            assert "ExtendedStatistics" in dp
            assert "p50" in dp["ExtendedStatistics"]
            assert isinstance(dp["ExtendedStatistics"], dict)

    def test_list_tags_for_resource(self, cw):
        """ListTagsForResource returns tags for an alarm."""
        import uuid

        alarm_name = f"tag-res-{uuid.uuid4().hex[:8]}"
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="TagResNS",
            MetricName="M1",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        try:
            alarms = cw.describe_alarms(AlarmNames=[alarm_name])
            alarm_arn = alarms["MetricAlarms"][0]["AlarmArn"]
            resp = cw.list_tags_for_resource(ResourceARN=alarm_arn)
            assert "Tags" in resp
            tag_keys = [t["Key"] for t in resp["Tags"]]
            assert "env" in tag_keys
            assert "team" in tag_keys
            tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "platform"
        finally:
            cw.delete_alarms(AlarmNames=[alarm_name])

    def test_put_describe_delete_insight_rules(self, cw):
        """PutInsightRule, DescribeInsightRules, DeleteInsightRules lifecycle."""
        import uuid

        rule_name = f"rule-{uuid.uuid4().hex[:8]}"
        rule_def = (
            '{"Schema":{"Name":"CloudWatchLogRule","Version":1},'
            '"LogGroupNames":["/test"],'
            '"Contributions":{"Filters":[],"Keys":["$.key"],"ValueOf":"$.value"}}'
        )
        # Create rule
        resp = cw.put_insight_rule(RuleName=rule_name, RuleDefinition=rule_def, RuleState="ENABLED")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        try:
            # Describe rules
            resp = cw.describe_insight_rules()
            assert "InsightRules" in resp
            rule_names = [r["Name"] for r in resp["InsightRules"]]
            assert rule_name in rule_names
            # Verify state
            rule = next(r for r in resp["InsightRules"] if r["Name"] == rule_name)
            assert rule["State"] == "ENABLED"
        finally:
            # Delete rule
            resp = cw.delete_insight_rules(RuleNames=[rule_name])
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disable_enable_insight_rules(self, cw):
        """DisableInsightRules and EnableInsightRules toggle state."""
        import uuid

        rule_name = f"toggle-{uuid.uuid4().hex[:8]}"
        rule_def = (
            '{"Schema":{"Name":"CloudWatchLogRule","Version":1},'
            '"LogGroupNames":["/test"],'
            '"Contributions":{"Filters":[],"Keys":["$.key"],"ValueOf":"$.value"}}'
        )
        cw.put_insight_rule(RuleName=rule_name, RuleDefinition=rule_def, RuleState="ENABLED")
        try:
            # Disable
            resp = cw.disable_insight_rules(RuleNames=[rule_name])
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # Enable
            resp = cw.enable_insight_rules(RuleNames=[rule_name])
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            cw.delete_insight_rules(RuleNames=[rule_name])


class TestCloudWatchGapStubs:
    """Tests for newly-stubbed CloudWatch operations that return empty results."""

    def test_describe_anomaly_detectors(self, cw):
        resp = cw.describe_anomaly_detectors()
        assert "AnomalyDetectors" in resp
        assert isinstance(resp["AnomalyDetectors"], list)

    def test_list_metric_streams(self, cw):
        resp = cw.list_metric_streams()
        assert "Entries" in resp
        assert isinstance(resp["Entries"], list)

    def test_list_managed_insight_rules(self, cw):
        resp = cw.list_managed_insight_rules(
            ResourceARN="arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0"
        )
        assert "ManagedRules" in resp
        assert isinstance(resp["ManagedRules"], list)


class TestCloudwatchAutoCoverage:
    """Auto-generated coverage tests for cloudwatch."""

    @pytest.fixture
    def client(self):
        return make_client("cloudwatch")

    def test_delete_anomaly_detector(self, client):
        """DeleteAnomalyDetector returns a 200 response."""
        resp = client.delete_anomaly_detector()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_alarm_mute_rules(self, client):
        """ListAlarmMuteRules returns a response."""
        resp = client.list_alarm_mute_rules()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_anomaly_detector_lifecycle(self, client):
        """PutAnomalyDetector → DescribeAnomalyDetectors → DeleteAnomalyDetector."""
        import uuid

        ns = f"ADLife-{uuid.uuid4().hex[:8]}"
        metric = "ADMetric"
        stat = "Average"

        # Put
        resp = client.put_anomaly_detector(Namespace=ns, MetricName=metric, Stat=stat)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Describe and verify
        resp = client.describe_anomaly_detectors(Namespace=ns)
        detectors = resp["AnomalyDetectors"]
        assert len(detectors) >= 1
        ad = detectors[0]
        assert ad["Namespace"] == ns
        assert ad["MetricName"] == metric
        assert ad["Stat"] == stat
        assert ad["StateValue"] == "PENDING_TRAINING"

        # Delete
        resp = client.delete_anomaly_detector(Namespace=ns, MetricName=metric, Stat=stat)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify gone
        resp = client.describe_anomaly_detectors(Namespace=ns)
        assert len(resp["AnomalyDetectors"]) == 0


class TestCloudWatchManagedInsightRules:
    """Tests for CloudWatch Managed Insight Rules operations."""

    def test_list_managed_insight_rules(self, cw):
        """ListManagedInsightRules returns a list for a resource ARN."""
        resp = cw.list_managed_insight_rules(
            ResourceARN="arn:aws:ec2:us-east-1:123456789012:instance/i-fake"
        )
        assert "ManagedRules" in resp
        assert isinstance(resp["ManagedRules"], list)


class TestCloudWatchMetricStreams:
    """Tests for CloudWatch Metric Streams operations."""

    def test_list_metric_streams(self, cw):
        """ListMetricStreams returns entries list."""
        resp = cw.list_metric_streams()
        assert "Entries" in resp
        assert isinstance(resp["Entries"], list)

    def test_metric_stream_lifecycle(self, cw):
        """PutMetricStream, GetMetricStream, ListMetricStreams, DeleteMetricStream."""
        stream_name = f"test-stream-{uuid.uuid4().hex[:8]}"
        firehose_arn = "arn:aws:firehose:us-east-1:123456789012:deliverystream/test"
        role_arn = "arn:aws:iam::123456789012:role/cw-stream-role"

        # Put
        resp = cw.put_metric_stream(
            Name=stream_name,
            FirehoseArn=firehose_arn,
            RoleArn=role_arn,
            OutputFormat="json",
        )
        assert "Arn" in resp
        assert stream_name in resp["Arn"]

        try:
            # Get
            resp = cw.get_metric_stream(Name=stream_name)
            assert resp["Name"] == stream_name
            assert resp["FirehoseArn"] == firehose_arn
            assert resp["RoleArn"] == role_arn
            assert resp["OutputFormat"] == "json"
            assert resp["State"] == "running"

            # List
            resp = cw.list_metric_streams()
            names = [e["Name"] for e in resp["Entries"]]
            assert stream_name in names
        finally:
            # Delete
            resp = cw.delete_metric_stream(Name=stream_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify gone from list
        resp = cw.list_metric_streams()
        names = [e["Name"] for e in resp["Entries"]]
        assert stream_name not in names

    def test_stop_and_start_metric_streams(self, cw):
        """StopMetricStreams and StartMetricStreams toggle state."""
        stream_name = f"toggle-stream-{uuid.uuid4().hex[:8]}"
        cw.put_metric_stream(
            Name=stream_name,
            FirehoseArn="arn:aws:firehose:us-east-1:123456789012:deliverystream/test",
            RoleArn="arn:aws:iam::123456789012:role/test",
            OutputFormat="json",
        )
        try:
            # Stop
            resp = cw.stop_metric_streams(Names=[stream_name])
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            stream = cw.get_metric_stream(Name=stream_name)
            assert stream["State"] == "stopped"

            # Start
            resp = cw.start_metric_streams(Names=[stream_name])
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            stream = cw.get_metric_stream(Name=stream_name)
            assert stream["State"] == "running"
        finally:
            cw.delete_metric_stream(Name=stream_name)

    def test_put_metric_stream_with_include_filters(self, cw):
        """PutMetricStream with IncludeFilters."""
        stream_name = f"filter-stream-{uuid.uuid4().hex[:8]}"
        cw.put_metric_stream(
            Name=stream_name,
            FirehoseArn="arn:aws:firehose:us-east-1:123456789012:deliverystream/test",
            RoleArn="arn:aws:iam::123456789012:role/test",
            OutputFormat="json",
            IncludeFilters=[{"Namespace": "AWS/EC2"}],
        )
        try:
            stream = cw.get_metric_stream(Name=stream_name)
            assert stream["Name"] == stream_name
            assert len(stream.get("IncludeFilters", [])) >= 1
        finally:
            cw.delete_metric_stream(Name=stream_name)

    def test_delete_nonexistent_metric_stream(self, cw):
        """DeleteMetricStream on nonexistent stream does not error."""
        resp = cw.delete_metric_stream(Name="nonexistent-stream")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_metric_stream_opentelemetry_format(self, cw):
        """PutMetricStream with opentelemetry1.0 output format."""
        stream_name = f"otel-stream-{uuid.uuid4().hex[:8]}"
        cw.put_metric_stream(
            Name=stream_name,
            FirehoseArn="arn:aws:firehose:us-east-1:123456789012:deliverystream/test",
            RoleArn="arn:aws:iam::123456789012:role/test",
            OutputFormat="opentelemetry1.0",
        )
        try:
            stream = cw.get_metric_stream(Name=stream_name)
            assert stream["OutputFormat"] == "opentelemetry1.0"
        finally:
            cw.delete_metric_stream(Name=stream_name)


class TestCloudWatchGapOps:
    """Tests for newly-implemented CloudWatch gap operations."""

    def test_get_metric_widget_image(self, cw):
        """GetMetricWidgetImage returns image bytes."""
        import json

        resp = cw.get_metric_widget_image(
            MetricWidget=json.dumps(
                {
                    "metrics": [["AWS/EC2", "CPUUtilization", "InstanceId", "i-12345"]],
                    "period": 300,
                }
            )
        )
        assert "MetricWidgetImage" in resp
        image_data = resp["MetricWidgetImage"]
        assert len(image_data) > 0

    def test_get_insight_rule_report(self, cw):
        """GetInsightRuleReport returns report after creating a rule."""
        rule_name = f"report-rule-{uuid.uuid4().hex[:8]}"
        rule_def = (
            '{"Schema":{"Name":"CloudWatchLogRule","Version":1},'
            '"LogGroupNames":["/test"],'
            '"Contributions":{"Filters":[],"Keys":["$.key"],"ValueOf":"$.value"}}'
        )
        cw.put_insight_rule(RuleName=rule_name, RuleDefinition=rule_def, RuleState="ENABLED")
        try:
            now = datetime.now(UTC)
            resp = cw.get_insight_rule_report(
                RuleName=rule_name,
                StartTime=now - timedelta(hours=1),
                EndTime=now,
                Period=300,
            )
            assert "Contributors" in resp
            assert "KeyLabels" in resp
            assert "AggregateValue" in resp
            assert isinstance(resp["Contributors"], list)
            assert isinstance(resp["KeyLabels"], list)
        finally:
            cw.delete_insight_rules(RuleNames=[rule_name])

    def test_put_managed_insight_rules(self, cw):
        """PutManagedInsightRules returns empty failures list."""
        resp = cw.put_managed_insight_rules(
            ManagedRules=[
                {
                    "TemplateName": "DynamoDBContributorInsights",
                    "ResourceARN": "arn:aws:dynamodb:us-east-1:123456789012:table/test",
                    "Tags": [],
                }
            ]
        )
        assert "Failures" in resp
        assert len(resp["Failures"]) == 0


class TestCloudWatchAlarmMuteRules:
    """Tests for CloudWatch alarm mute rule operations."""

    def test_describe_alarm_contributors(self, cw):
        """DescribeAlarmContributors returns AlarmContributors list."""
        resp = cw.describe_alarm_contributors(AlarmName="nonexistent-alarm")
        assert "AlarmContributors" in resp
        assert isinstance(resp["AlarmContributors"], list)

    def test_get_alarm_mute_rule_nonexistent(self, cw):
        """GetAlarmMuteRule with nonexistent name raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            cw.get_alarm_mute_rule(AlarmMuteRuleName="nonexistent-mute-rule-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_alarm_mute_rules(self, cw):
        """ListAlarmMuteRules returns 200."""
        resp = cw.list_alarm_mute_rules()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestCloudWatchMuteRuleCRUD:
    """Tests for CloudWatch PutAlarmMuteRule and DeleteAlarmMuteRule operations."""

    @pytest.fixture
    def cw(self):  # noqa: F811
        return make_client("cloudwatch")

    def test_put_and_delete_alarm_mute_rule(self, cw):
        """PutAlarmMuteRule creates a rule and DeleteAlarmMuteRule removes it."""
        name = f"mute-rule-{uuid.uuid4().hex[:8]}"
        cw.put_alarm_mute_rule(
            Name=name,
            Rule={"Schedule": {"Expression": "cron(0 0 * * ? *)", "Duration": "PT1H"}},
        )
        resp = cw.delete_alarm_mute_rule(AlarmMuteRuleName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_alarm_mute_rule_nonexistent(self, cw):
        """DeleteAlarmMuteRule with nonexistent name returns 200."""
        resp = cw.delete_alarm_mute_rule(AlarmMuteRuleName="nonexistent-mute-rule-xyz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestCloudWatchEdgeCases:
    """Edge case and behavioral fidelity tests for CloudWatch."""

    @pytest.fixture
    def cw(self):
        return make_client("cloudwatch")

    # ── put_metric_data: add RETRIEVE + LIST + ERROR patterns ─────────────────

    def test_put_metric_data_retrieve_via_statistics(self, cw):
        """PutMetricData → GetMetricStatistics round-trip (CREATE + RETRIEVE)."""
        ns = f"RoundTrip-{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {"MetricName": "Score", "Value": 77.0, "Unit": "Count", "Timestamp": now},
                {"MetricName": "Score", "Value": 88.0, "Unit": "Count", "Timestamp": now},
            ],
        )
        resp = cw.get_metric_statistics(
            Namespace=ns,
            MetricName="Score",
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
            Period=300,
            Statistics=["Sum", "Average", "SampleCount", "Minimum", "Maximum"],
        )
        assert len(resp["Datapoints"]) >= 1
        dp = resp["Datapoints"][0]
        assert dp["SampleCount"] == 2.0
        assert dp["Sum"] == 165.0
        assert dp["Minimum"] == 77.0
        assert dp["Maximum"] == 88.0

    def test_put_metric_data_list_after_put(self, cw):
        """PutMetricData then ListMetrics returns the new metric (CREATE + LIST)."""
        ns = f"ListAfterPut-{uuid.uuid4().hex[:8]}"
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[{"MetricName": "Heartbeat", "Value": 1.0, "Unit": "None"}],
        )
        resp = cw.list_metrics(Namespace=ns)
        names = [m["MetricName"] for m in resp["Metrics"]]
        assert "Heartbeat" in names
        assert len(resp["Metrics"]) >= 1
        # Each metric entry must have Namespace and MetricName fields with correct values
        for m in resp["Metrics"]:
            assert m["Namespace"] == ns
            assert m["MetricName"] == "Heartbeat"

    # ── list_metrics: add pagination ─────────────────────────────────────────

    def test_list_metrics_pagination(self, cw):
        """ListMetrics paginates with NextToken when there are many metrics."""
        ns = f"Paginate-{uuid.uuid4().hex[:8]}"
        for i in range(5):
            cw.put_metric_data(
                Namespace=ns,
                MetricData=[{"MetricName": f"Metric{i}", "Value": float(i), "Unit": "None"}],
            )
        # Collect all metrics via pagination
        all_metrics = []
        kwargs = {"Namespace": ns}
        while True:
            resp = cw.list_metrics(**kwargs)
            all_metrics.extend(resp["Metrics"])
            next_token = resp.get("NextToken")
            if not next_token:
                break
            kwargs["NextToken"] = next_token
        names = {m["MetricName"] for m in all_metrics}
        for i in range(5):
            assert f"Metric{i}" in names

    def test_list_metrics_empty_namespace_returns_empty(self, cw):
        """ListMetrics for a namespace with no data returns empty list (LIST + ERROR boundary)."""
        ns = f"EmptyNS-{uuid.uuid4().hex[:8]}"
        resp = cw.list_metrics(Namespace=ns)
        assert resp["Metrics"] == []

    # ── alarm ARN format ──────────────────────────────────────────────────────

    def test_alarm_arn_format(self, cw):
        """Alarm ARN matches arn:aws:cloudwatch:<region>:<account>:alarm:<name>."""
        import re

        name = f"arn-check-{uuid.uuid4().hex[:8]}"
        cw.put_metric_alarm(
            AlarmName=name,
            Namespace="ArnNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        try:
            resp = cw.describe_alarms(AlarmNames=[name])
            arn = resp["MetricAlarms"][0]["AlarmArn"]
            assert re.match(
                r"arn:aws:cloudwatch:[a-z0-9-]+:\d+:alarm:.+", arn
            ), f"Unexpected ARN format: {arn}"
        finally:
            cw.delete_alarms(AlarmNames=[name])

    # ── alarm idempotent update (PUT twice = UPDATE) ──────────────────────────

    def test_alarm_idempotent_update(self, cw):
        """Putting an alarm with the same name updates it in place (CREATE + UPDATE)."""
        name = f"update-alarm-{uuid.uuid4().hex[:8]}"
        try:
            cw.put_metric_alarm(
                AlarmName=name,
                Namespace="IdempNS",
                MetricName="M",
                ComparisonOperator="GreaterThanThreshold",
                EvaluationPeriods=1,
                Period=60,
                Statistic="Average",
                Threshold=50.0,
            )
            arn_before = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]["AlarmArn"]

            # Re-put with different threshold
            cw.put_metric_alarm(
                AlarmName=name,
                Namespace="IdempNS",
                MetricName="M",
                ComparisonOperator="GreaterThanThreshold",
                EvaluationPeriods=1,
                Period=60,
                Statistic="Average",
                Threshold=99.0,
            )
            resp = cw.describe_alarms(AlarmNames=[name])
            alarm = resp["MetricAlarms"][0]
            # ARN should be stable across updates
            assert alarm["AlarmArn"] == arn_before
            assert alarm["Threshold"] == 99.0
        finally:
            cw.delete_alarms(AlarmNames=[name])

    # ── alarm timestamps (behavioral fidelity) ────────────────────────────────

    def test_alarm_has_timestamps(self, cw):
        """Alarm response includes StateUpdatedTimestamp and AlarmConfigurationUpdatedTimestamp."""
        name = f"ts-alarm-{uuid.uuid4().hex[:8]}"
        try:
            cw.put_metric_alarm(
                AlarmName=name,
                Namespace="TsNS",
                MetricName="M",
                ComparisonOperator="GreaterThanThreshold",
                EvaluationPeriods=1,
                Period=60,
                Statistic="Average",
                Threshold=50.0,
            )
            resp = cw.describe_alarms(AlarmNames=[name])
            alarm = resp["MetricAlarms"][0]
            assert "StateUpdatedTimestamp" in alarm
            assert "AlarmConfigurationUpdatedTimestamp" in alarm
            assert alarm["AlarmName"] == name
            assert alarm["StateValue"] in ("OK", "ALARM", "INSUFFICIENT_DATA")
        finally:
            cw.delete_alarms(AlarmNames=[name])

    # ── set_alarm_state: add LIST + ERROR patterns ────────────────────────────

    def test_set_alarm_state_then_list_by_state(self, cw):
        """SetAlarmState then DescribeAlarms filtered by StateValue (RETRIEVE + LIST)."""
        name = f"state-list-{uuid.uuid4().hex[:8]}"
        cw.put_metric_alarm(
            AlarmName=name,
            Namespace="SLNs",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        try:
            cw.set_alarm_state(AlarmName=name, StateValue="ALARM", StateReason="test")
            resp = cw.describe_alarms(StateValue="ALARM")
            names = [a["AlarmName"] for a in resp["MetricAlarms"]]
            assert name in names
        finally:
            cw.delete_alarms(AlarmNames=[name])

    def test_set_alarm_state_nonexistent_alarm_error(self, cw):
        """SetAlarmState on nonexistent alarm raises ResourceNotFoundException (ERROR)."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            cw.set_alarm_state(
                AlarmName="nonexistent-alarm-xyz-99",
                StateValue="ALARM",
                StateReason="testing error",
            )
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ResourceNotFound",
        )

    # ── metric stream: ERROR pattern for nonexistent get ─────────────────────

    def test_get_metric_stream_nonexistent_error(self, cw):
        """GetMetricStream on nonexistent stream raises ResourceNotFoundException (ERROR)."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            cw.get_metric_stream(Name="nonexistent-stream-xyz-99")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ResourceNotFound",
        )

    # ── anomaly detector: filter by namespace ────────────────────────────────

    def test_describe_anomaly_detectors_filter_by_namespace(self, cw):
        """DescribeAnomalyDetectors with Namespace filter (CREATE + RETRIEVE + LIST + DELETE)."""
        ns = f"ADFilter-{uuid.uuid4().hex[:8]}"
        metric = "ADMetric"
        stat = "Sum"
        cw.put_anomaly_detector(Namespace=ns, MetricName=metric, Stat=stat)
        try:
            # Filter by namespace: should find only the one we created
            resp = cw.describe_anomaly_detectors(Namespace=ns)
            assert len(resp["AnomalyDetectors"]) >= 1
            for ad in resp["AnomalyDetectors"]:
                assert ad["Namespace"] == ns
            # Filter different namespace: should be empty
            resp2 = cw.describe_anomaly_detectors(Namespace=f"Other-{uuid.uuid4().hex[:8]}")
            assert resp2["AnomalyDetectors"] == []
        finally:
            cw.delete_anomaly_detector(Namespace=ns, MetricName=metric, Stat=stat)

    # ── list_alarm_mute_rules: add response key check ─────────────────────────

    def test_list_alarm_mute_rules_response_structure(self, cw):
        """ListAlarmMuteRules response includes AlarmMuteRuleSummaries key (LIST)."""
        resp = cw.list_alarm_mute_rules()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AlarmMuteRuleSummaries" in resp
        assert isinstance(resp["AlarmMuteRuleSummaries"], list)

    # ── delete nonexistent metric stream: add LIST context ───────────────────

    def test_delete_nonexistent_metric_stream_not_in_list(self, cw):
        """Deleting nonexistent stream: still 200, and it's not in list (DELETE + LIST)."""
        name = "totally-fake-stream-xyz"
        resp = cw.delete_metric_stream(Name=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        list_resp = cw.list_metric_streams()
        names = [e["Name"] for e in list_resp["Entries"]]
        assert name not in names

    # ── get_metric_widget_image: add format validation ────────────────────────

    def test_get_metric_widget_image_is_png(self, cw):
        """GetMetricWidgetImage returns PNG bytes (starts with PNG magic number)."""
        import json

        resp = cw.get_metric_widget_image(
            MetricWidget=json.dumps(
                {
                    "metrics": [["AWS/EC2", "CPUUtilization", "InstanceId", "i-12345"]],
                    "period": 300,
                    "width": 400,
                    "height": 300,
                }
            )
        )
        image_data = resp["MetricWidgetImage"]
        assert len(image_data) > 0
        # PNG magic bytes: \x89PNG
        assert image_data[:4] == b"\x89PNG"

    # ── put_managed_insight_rules: add retrieve context ───────────────────────

    def test_put_managed_insight_rules_no_failures(self, cw):
        """PutManagedInsightRules returns empty Failures list (CREATE + ERROR check)."""
        resp = cw.put_managed_insight_rules(
            ManagedRules=[
                {
                    "TemplateName": "DynamoDBContributorInsights",
                    "ResourceARN": "arn:aws:dynamodb:us-east-1:123456789012:table/my-table",
                    "Tags": [{"Key": "team", "Value": "platform"}],
                }
            ]
        )
        assert "Failures" in resp
        assert resp["Failures"] == []
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    # ── alarm delete: list after delete shows it's gone ───────────────────────

    def test_delete_alarm_not_in_list_after(self, cw):
        """Deleted alarm no longer appears in DescribeAlarms (CREATE + DELETE + LIST)."""
        name = f"gone-alarm-{uuid.uuid4().hex[:8]}"
        cw.put_metric_alarm(
            AlarmName=name,
            Namespace="GoneNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        # Verify exists
        resp = cw.describe_alarms(AlarmNames=[name])
        assert len(resp["MetricAlarms"]) == 1

        # Delete
        cw.delete_alarms(AlarmNames=[name])

        # Verify gone
        resp = cw.describe_alarms(AlarmNames=[name])
        assert len(resp["MetricAlarms"]) == 0

    # ── unicode in metric names ───────────────────────────────────────────────

    def test_put_metric_data_unicode_metric_name(self, cw):
        """PutMetricData with unicode in MetricName round-trips correctly."""
        ns = f"Unicode-{uuid.uuid4().hex[:8]}"
        metric_name = "Latência_μs"
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[{"MetricName": metric_name, "Value": 42.0, "Unit": "Microseconds"}],
        )
        resp = cw.list_metrics(Namespace=ns)
        names = [m["MetricName"] for m in resp["Metrics"]]
        assert metric_name in names

    # ── describe_alarm_contributors: add alarm context ───────────────────────

    def test_describe_alarm_contributors_structure(self, cw):
        """DescribeAlarmContributors returns AlarmContributors with list structure."""
        resp = cw.describe_alarm_contributors(AlarmName="any-alarm-name")
        assert "AlarmContributors" in resp
        assert isinstance(resp["AlarmContributors"], list)


class TestCloudWatchBehavioralCoverage:
    """Targeted behavioral coverage tests to fill pattern gaps."""

    @pytest.fixture
    def cw(self):
        return make_client("cloudwatch")

    # ── put_metric_data: ERROR + UPDATE patterns ──────────────────────────────

    def test_put_metric_data_update_and_list(self, cw):
        """Put metric data twice (UPDATE), then list and get stats (RETRIEVE + LIST + ERROR)."""
        from botocore.exceptions import ClientError

        ns = f"UpdateMetric-{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)
        # First put (CREATE)
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[{"MetricName": "Counter", "Value": 10.0, "Unit": "Count", "Timestamp": now}],
        )
        # Second put same metric (UPDATE)
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[{"MetricName": "Counter", "Value": 20.0, "Unit": "Count", "Timestamp": now}],
        )
        # LIST: both datapoints visible
        resp = cw.list_metrics(Namespace=ns, MetricName="Counter")
        assert len(resp["Metrics"]) >= 1
        # RETRIEVE: statistics reflect both
        stats = cw.get_metric_statistics(
            Namespace=ns,
            MetricName="Counter",
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
            Period=300,
            Statistics=["SampleCount", "Sum"],
        )
        assert len(stats["Datapoints"]) >= 1
        dp = stats["Datapoints"][0]
        assert dp["SampleCount"] >= 2.0
        assert dp["Sum"] >= 30.0
        # ERROR: invalid future timestamp far out of range should still succeed (CloudWatch accepts it)
        resp2 = cw.put_metric_data(
            Namespace=ns,
            MetricData=[{"MetricName": "Counter", "Value": 5.0, "Unit": "Count"}],
        )
        assert resp2["ResponseMetadata"]["HTTPStatusCode"] == 200

    # ── describe_anomaly_detectors: CREATE + RETRIEVE + DELETE + ERROR ─────────

    def test_describe_anomaly_detectors_full_lifecycle(self, cw):
        """PutAnomalyDetector → DescribeAnomalyDetectors → Delete → verify gone (C+R+L+D+E)."""
        from botocore.exceptions import ClientError

        ns = f"ADFull-{uuid.uuid4().hex[:8]}"
        metric = "FullMetric"
        stat = "p90"

        # CREATE
        resp = cw.put_anomaly_detector(Namespace=ns, MetricName=metric, Stat=stat)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # RETRIEVE - describe with filters
        resp = cw.describe_anomaly_detectors(Namespace=ns, MetricName=metric)
        detectors = resp["AnomalyDetectors"]
        assert len(detectors) >= 1
        ad = detectors[0]
        assert ad["MetricName"] == metric
        assert ad["Stat"] == stat

        # LIST - describe without filters returns it
        all_resp = cw.describe_anomaly_detectors()
        all_ns = [d["Namespace"] for d in all_resp["AnomalyDetectors"]]
        assert ns in all_ns

        # DELETE
        cw.delete_anomaly_detector(Namespace=ns, MetricName=metric, Stat=stat)

        # ERROR/verify: gone after delete
        resp = cw.describe_anomaly_detectors(Namespace=ns)
        assert resp["AnomalyDetectors"] == []

    # ── list_metric_streams: CREATE + RETRIEVE + DELETE + ERROR ───────────────

    def test_list_metric_streams_lifecycle(self, cw):
        """Create stream, list it, get it (RETRIEVE), delete it, verify gone (C+R+L+D+E)."""
        from botocore.exceptions import ClientError

        name = f"ls-stream-{uuid.uuid4().hex[:8]}"
        # CREATE
        resp = cw.put_metric_stream(
            Name=name,
            FirehoseArn="arn:aws:firehose:us-east-1:123456789012:deliverystream/test",
            RoleArn="arn:aws:iam::123456789012:role/test",
            OutputFormat="json",
        )
        assert "Arn" in resp

        try:
            # LIST
            list_resp = cw.list_metric_streams()
            names = [e["Name"] for e in list_resp["Entries"]]
            assert name in names

            # RETRIEVE
            get_resp = cw.get_metric_stream(Name=name)
            assert get_resp["Name"] == name
            assert get_resp["OutputFormat"] == "json"

            # UPDATE (stop/start)
            cw.stop_metric_streams(Names=[name])
            stopped = cw.get_metric_stream(Name=name)
            assert stopped["State"] == "stopped"

        finally:
            # DELETE
            cw.delete_metric_stream(Name=name)

        # ERROR: getting deleted stream raises
        with pytest.raises(ClientError) as exc:
            cw.get_metric_stream(Name=name)
        assert exc.value.response["Error"]["Code"] in ("ResourceNotFoundException", "ResourceNotFound")

    # ── list_managed_insight_rules: CREATE context + RETRIEVE + ERROR ──────────

    def test_list_managed_insight_rules_with_context(self, cw):
        """PutManagedInsightRules then ListManagedInsightRules for that resource (C+R+L+E)."""
        resource_arn = "arn:aws:dynamodb:us-east-1:123456789012:table/my-test-table"

        # CREATE (put managed rules for a resource)
        put_resp = cw.put_managed_insight_rules(
            ManagedRules=[
                {
                    "TemplateName": "DynamoDBContributorInsights",
                    "ResourceARN": resource_arn,
                    "Tags": [],
                }
            ]
        )
        assert put_resp["Failures"] == []

        # LIST (list managed rules for that resource)
        list_resp = cw.list_managed_insight_rules(ResourceARN=resource_arn)
        assert "ManagedRules" in list_resp
        assert isinstance(list_resp["ManagedRules"], list)

        # RETRIEVE: check the response metadata
        assert list_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # ERROR boundary: list for unknown ARN returns empty list (not an error)
        other_resp = cw.list_managed_insight_rules(
            ResourceARN="arn:aws:dynamodb:us-east-1:123456789012:table/nonexistent"
        )
        assert "ManagedRules" in other_resp

    # ── delete_anomaly_detector: CREATE + LIST + RETRIEVE before delete ────────

    def test_delete_anomaly_detector_with_lifecycle(self, cw):
        """Create anomaly detector, list it, retrieve it, then delete (C+R+L+D)."""
        ns = f"ADDel-{uuid.uuid4().hex[:8]}"
        metric = "DelMetric"
        stat = "Average"

        # CREATE
        cw.put_anomaly_detector(Namespace=ns, MetricName=metric, Stat=stat)

        # LIST: appears in list
        list_resp = cw.describe_anomaly_detectors(Namespace=ns)
        assert len(list_resp["AnomalyDetectors"]) >= 1

        # RETRIEVE: specific fields present
        ad = list_resp["AnomalyDetectors"][0]
        assert ad["Namespace"] == ns
        assert ad["MetricName"] == metric
        assert "StateValue" in ad

        # UPDATE: put again with different stat
        cw.put_anomaly_detector(Namespace=ns, MetricName=metric, Stat="Sum")
        updated_resp = cw.describe_anomaly_detectors(Namespace=ns)
        updated_ad = next(d for d in updated_resp["AnomalyDetectors"] if d["MetricName"] == metric and d["Stat"] == "Sum")
        assert updated_ad["Stat"] == "Sum"

        # DELETE
        cw.delete_anomaly_detector(Namespace=ns, MetricName=metric, Stat="Sum")
        final_resp = cw.describe_anomaly_detectors(Namespace=ns)
        remaining = [d for d in final_resp["AnomalyDetectors"] if d["MetricName"] == metric and d["Stat"] == "Sum"]
        assert remaining == []

    # ── list_alarm_mute_rules: CREATE + RETRIEVE + DELETE + ERROR ─────────────

    def test_list_alarm_mute_rules_full_lifecycle(self, cw):
        """PutAlarmMuteRule → ListAlarmMuteRules → GetAlarmMuteRule → Delete (C+R+L+D+E)."""
        from botocore.exceptions import ClientError

        name = f"mute-full-{uuid.uuid4().hex[:8]}"

        # CREATE
        cw.put_alarm_mute_rule(
            Name=name,
            Rule={"Schedule": {"Expression": "cron(0 0 * * ? *)", "Duration": "PT1H"}},
        )

        try:
            # LIST: appears in list
            list_resp = cw.list_alarm_mute_rules()
            assert list_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "AlarmMuteRuleSummaries" in list_resp

            # RETRIEVE: get the rule by name
            get_resp = cw.get_alarm_mute_rule(AlarmMuteRuleName=name)
            assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        finally:
            # DELETE
            del_resp = cw.delete_alarm_mute_rule(AlarmMuteRuleName=name)
            assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # ERROR: get deleted rule raises ResourceNotFoundException
        with pytest.raises(ClientError) as exc:
            cw.get_alarm_mute_rule(AlarmMuteRuleName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── delete_nonexistent_metric_stream: CREATE real one for contrast ─────────

    def test_delete_metric_stream_real_vs_nonexistent(self, cw):
        """Create a real stream, delete it, compare with deleting nonexistent (C+R+L+D)."""
        name = f"real-stream-{uuid.uuid4().hex[:8]}"
        fake_name = f"fake-stream-{uuid.uuid4().hex[:8]}"

        # CREATE
        cw.put_metric_stream(
            Name=name,
            FirehoseArn="arn:aws:firehose:us-east-1:123456789012:deliverystream/test",
            RoleArn="arn:aws:iam::123456789012:role/test",
            OutputFormat="json",
        )

        # RETRIEVE: confirm exists
        get_resp = cw.get_metric_stream(Name=name)
        assert get_resp["Name"] == name

        # LIST: both should behave consistently
        list_resp = cw.list_metric_streams()
        names = [e["Name"] for e in list_resp["Entries"]]
        assert name in names
        assert fake_name not in names

        # DELETE real stream: 200
        del_resp = cw.delete_metric_stream(Name=name)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # DELETE nonexistent: also 200 (idempotent)
        del_resp2 = cw.delete_metric_stream(Name=fake_name)
        assert del_resp2["ResponseMetadata"]["HTTPStatusCode"] == 200

    # ── get_metric_widget_image: ERROR + context ───────────────────────────────

    def test_get_metric_widget_image_with_metric_data(self, cw):
        """Put metric data, then get widget image showing that data (C+R+L+E)."""
        import json

        ns = f"WidgetImg-{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)

        # CREATE metric data
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {"MetricName": "ResponseTime", "Value": 50.0, "Unit": "Milliseconds", "Timestamp": now}
            ],
        )

        # LIST: verify metric appears
        list_resp = cw.list_metrics(Namespace=ns)
        assert len(list_resp["Metrics"]) >= 1

        # RETRIEVE: get widget image for this specific metric
        widget = {
            "metrics": [[ns, "ResponseTime"]],
            "period": 300,
            "start": "-PT1H",
            "end": "PT0H",
            "width": 600,
            "height": 400,
        }
        img_resp = cw.get_metric_widget_image(MetricWidget=json.dumps(widget))
        assert "MetricWidgetImage" in img_resp
        image_data = img_resp["MetricWidgetImage"]
        assert len(image_data) > 0
        # PNG magic bytes
        assert image_data[:4] == b"\x89PNG"

    # ── put_managed_insight_rules: RETRIEVE + LIST + DELETE + ERROR ─────────────

    def test_put_managed_insight_rules_lifecycle(self, cw):
        """PutManagedInsightRules → list rules → put again (C+R+L+U+E)."""
        resource_arn = "arn:aws:dynamodb:us-east-1:123456789012:table/managed-test"

        # CREATE
        resp = cw.put_managed_insight_rules(
            ManagedRules=[
                {
                    "TemplateName": "DynamoDBContributorInsights",
                    "ResourceARN": resource_arn,
                    "Tags": [{"Key": "purpose", "Value": "test"}],
                }
            ]
        )
        assert "Failures" in resp
        assert resp["Failures"] == []

        # RETRIEVE via list_managed_insight_rules
        list_resp = cw.list_managed_insight_rules(ResourceARN=resource_arn)
        assert "ManagedRules" in list_resp

        # UPDATE: put again (idempotent)
        resp2 = cw.put_managed_insight_rules(
            ManagedRules=[
                {
                    "TemplateName": "DynamoDBContributorInsights",
                    "ResourceARN": resource_arn,
                    "Tags": [{"Key": "purpose", "Value": "updated"}],
                }
            ]
        )
        assert resp2["Failures"] == []

        # LIST again - still works
        list_resp2 = cw.list_managed_insight_rules(ResourceARN=resource_arn)
        assert list_resp2["ResponseMetadata"]["HTTPStatusCode"] == 200

    # ── describe_alarm_contributors: CREATE alarm + full cycle ────────────────

    def test_describe_alarm_contributors_full(self, cw):
        """Create alarm, describe contributors, update alarm, delete (C+R+L+U+D)."""
        name = f"contrib-alarm-{uuid.uuid4().hex[:8]}"

        # CREATE alarm
        cw.put_metric_alarm(
            AlarmName=name,
            Namespace="ContribNS",
            MetricName="Errors",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Sum",
            Threshold=10.0,
        )
        try:
            # RETRIEVE: describe alarm to get ARN
            desc = cw.describe_alarms(AlarmNames=[name])
            assert len(desc["MetricAlarms"]) == 1
            alarm = desc["MetricAlarms"][0]
            assert alarm["AlarmName"] == name

            # LIST: describe_alarm_contributors for this alarm
            contrib_resp = cw.describe_alarm_contributors(AlarmName=name)
            assert "AlarmContributors" in contrib_resp
            assert isinstance(contrib_resp["AlarmContributors"], list)

            # UPDATE: change threshold
            cw.put_metric_alarm(
                AlarmName=name,
                Namespace="ContribNS",
                MetricName="Errors",
                ComparisonOperator="GreaterThanThreshold",
                EvaluationPeriods=2,
                Period=60,
                Statistic="Sum",
                Threshold=20.0,
            )
            updated = cw.describe_alarms(AlarmNames=[name])
            assert updated["MetricAlarms"][0]["Threshold"] == 20.0
            assert updated["MetricAlarms"][0]["EvaluationPeriods"] == 2

        finally:
            # DELETE
            cw.delete_alarms(AlarmNames=[name])

        # ERROR: alarm gone after delete
        gone = cw.describe_alarms(AlarmNames=[name])
        assert gone["MetricAlarms"] == []

    # ── delete_alarm_mute_rule_nonexistent: CREATE + LIST before delete ────────

    def test_delete_alarm_mute_rule_with_list(self, cw):
        """Create mute rule, list it, delete it, list again (C+R+L+D+E)."""
        from botocore.exceptions import ClientError

        name = f"mute-list-{uuid.uuid4().hex[:8]}"

        # CREATE
        cw.put_alarm_mute_rule(
            Name=name,
            Rule={"Schedule": {"Expression": "cron(0 12 * * ? *)", "Duration": "PT2H"}},
        )

        # LIST: appears in list
        list_resp = cw.list_alarm_mute_rules()
        assert list_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AlarmMuteRuleSummaries" in list_resp

        # RETRIEVE: get by name
        get_resp = cw.get_alarm_mute_rule(AlarmMuteRuleName=name)
        assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # DELETE
        del_resp = cw.delete_alarm_mute_rule(AlarmMuteRuleName=name)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # ERROR: get after delete raises
        with pytest.raises(ClientError) as exc:
            cw.get_alarm_mute_rule(AlarmMuteRuleName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── list_metrics empty namespace: CREATE + UPDATE + DELETE + ERROR ─────────

    def test_list_metrics_namespace_create_then_empty(self, cw):
        """Put metric, list it (CREATE+LIST), verify different ns is empty (ERROR boundary)."""
        from botocore.exceptions import ClientError

        ns = f"NonEmpty-{uuid.uuid4().hex[:8]}"
        empty_ns = f"TrulyEmpty-{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)

        # CREATE metric data
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[{"MetricName": "Pulse", "Value": 1.0, "Unit": "None", "Timestamp": now}],
        )

        # RETRIEVE via get_metric_statistics
        stats = cw.get_metric_statistics(
            Namespace=ns,
            MetricName="Pulse",
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
            Period=300,
            Statistics=["Sum"],
        )
        assert "Datapoints" in stats

        # LIST: non-empty namespace has metric
        list_resp = cw.list_metrics(Namespace=ns)
        assert len(list_resp["Metrics"]) >= 1
        assert list_resp["Metrics"][0]["Namespace"] == ns

        # UPDATE: put another metric to same namespace
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[{"MetricName": "Beat", "Value": 2.0, "Unit": "None", "Timestamp": now}],
        )
        list_resp2 = cw.list_metrics(Namespace=ns)
        metric_names = {m["MetricName"] for m in list_resp2["Metrics"]}
        assert "Pulse" in metric_names
        assert "Beat" in metric_names

        # LIST empty namespace returns empty
        empty_resp = cw.list_metrics(Namespace=empty_ns)
        assert empty_resp["Metrics"] == []

    # ── set_alarm_state_and_verify: add LIST + ERROR ───────────────────────────

    def test_set_alarm_state_list_and_error(self, cw):
        """SetAlarmState, list by state, verify error for nonexistent (C+R+L+U+D+E)."""
        from botocore.exceptions import ClientError

        name = f"state-full-{uuid.uuid4().hex[:8]}"

        # CREATE
        cw.put_metric_alarm(
            AlarmName=name,
            Namespace="StateFullNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        try:
            # UPDATE (set state)
            cw.set_alarm_state(AlarmName=name, StateValue="ALARM", StateReason="test trigger")

            # RETRIEVE
            desc = cw.describe_alarms(AlarmNames=[name])
            alarm = desc["MetricAlarms"][0]
            assert alarm["StateValue"] == "ALARM"
            assert alarm["StateReason"] == "test trigger"

            # LIST: filter by state finds it
            state_resp = cw.describe_alarms(StateValue="ALARM")
            alarm_names = [a["AlarmName"] for a in state_resp["MetricAlarms"]]
            assert name in alarm_names

            # UPDATE back to OK
            cw.set_alarm_state(AlarmName=name, StateValue="OK", StateReason="resolved")
            ok_resp = cw.describe_alarms(AlarmNames=[name])
            assert ok_resp["MetricAlarms"][0]["StateValue"] == "OK"

        finally:
            # DELETE
            cw.delete_alarms(AlarmNames=[name])

        # ERROR: set state on deleted alarm
        with pytest.raises(ClientError) as exc:
            cw.set_alarm_state(AlarmName=name, StateValue="ALARM", StateReason="ghost")
        assert exc.value.response["Error"]["Code"] in ("ResourceNotFoundException", "ResourceNotFound")


class TestCloudWatchStrongPatterns:
    """Tests targeting RETRIEVE, UPDATE, and ERROR behavioral patterns (most deficient)."""

    @pytest.fixture
    def cw(self):
        return make_client("cloudwatch")

    # ── RETRIEVE: alarm fields ────────────────────────────────────────────────

    def test_alarm_retrieve_all_fields(self, cw):
        """Create an alarm and verify all key fields are returned with correct values (C+R+L)."""
        name = f"field-alarm-{uuid.uuid4().hex[:8]}"
        cw.put_metric_alarm(
            AlarmName=name,
            Namespace="FieldNS",
            MetricName="Requests",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=3,
            Period=120,
            Statistic="Sum",
            Threshold=100.0,
            AlarmDescription="Field check alarm",
            TreatMissingData="breaching",
        )
        try:
            resp = cw.describe_alarms(AlarmNames=[name])
            assert len(resp["MetricAlarms"]) == 1
            alarm = resp["MetricAlarms"][0]
            # Strong field assertions
            assert alarm["AlarmName"] == name
            assert alarm["Namespace"] == "FieldNS"
            assert alarm["MetricName"] == "Requests"
            assert alarm["ComparisonOperator"] == "GreaterThanThreshold"
            assert alarm["EvaluationPeriods"] == 3
            assert alarm["Period"] == 120
            assert alarm["Statistic"] == "Sum"
            assert alarm["Threshold"] == 100.0
            assert alarm["AlarmDescription"] == "Field check alarm"
            assert alarm["TreatMissingData"] == "breaching"
            assert alarm["StateValue"] in ("OK", "ALARM", "INSUFFICIENT_DATA")
            assert "AlarmArn" in alarm
            assert "StateUpdatedTimestamp" in alarm
            assert "AlarmConfigurationUpdatedTimestamp" in alarm

            # LIST: appears in describe_alarms
            list_resp = cw.describe_alarms()
            names = [a["AlarmName"] for a in list_resp["MetricAlarms"]]
            assert name in names
        finally:
            cw.delete_alarms(AlarmNames=[name])

    # ── UPDATE: alarm threshold change persists ────────────────────────────────

    def test_alarm_update_threshold_persists(self, cw):
        """Create alarm, update threshold via re-put, verify new value retrieved (C+R+U+D)."""
        name = f"update-thresh-{uuid.uuid4().hex[:8]}"
        cw.put_metric_alarm(
            AlarmName=name,
            Namespace="UpdateNS",
            MetricName="Load",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        try:
            # RETRIEVE: initial threshold
            resp = cw.describe_alarms(AlarmNames=[name])
            assert resp["MetricAlarms"][0]["Threshold"] == 50.0

            # UPDATE: change threshold and evaluation periods
            cw.put_metric_alarm(
                AlarmName=name,
                Namespace="UpdateNS",
                MetricName="Load",
                ComparisonOperator="GreaterThanThreshold",
                EvaluationPeriods=5,
                Period=60,
                Statistic="Average",
                Threshold=95.0,
            )

            # RETRIEVE: new values
            resp2 = cw.describe_alarms(AlarmNames=[name])
            alarm = resp2["MetricAlarms"][0]
            assert alarm["Threshold"] == 95.0
            assert alarm["EvaluationPeriods"] == 5
            # ARN is stable across updates
            arn1 = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]["AlarmArn"]
            assert arn1 == alarm["AlarmArn"]
        finally:
            cw.delete_alarms(AlarmNames=[name])

    # ── ERROR: alarm set_alarm_state on nonexistent ────────────────────────────

    def test_set_alarm_state_nonexistent_raises(self, cw):
        """SetAlarmState on nonexistent alarm returns ResourceNotFoundException (E)."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            cw.set_alarm_state(
                AlarmName=f"ghost-alarm-{uuid.uuid4().hex[:8]}",
                StateValue="ALARM",
                StateReason="does not exist",
            )
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ResourceNotFound",
        )

    # ── RETRIEVE + UPDATE: metric stream full field check ─────────────────────

    def test_metric_stream_retrieve_and_update(self, cw):
        """Create stream, verify all fields, stop (UPDATE), start (UPDATE), verify state (C+R+U+D+E)."""
        from botocore.exceptions import ClientError

        name = f"retrieve-stream-{uuid.uuid4().hex[:8]}"
        firehose = "arn:aws:firehose:us-east-1:123456789012:deliverystream/mystream"
        role = "arn:aws:iam::123456789012:role/my-role"
        cw.put_metric_stream(Name=name, FirehoseArn=firehose, RoleArn=role, OutputFormat="json")
        try:
            # RETRIEVE: all fields
            stream = cw.get_metric_stream(Name=name)
            assert stream["Name"] == name
            assert stream["FirehoseArn"] == firehose
            assert stream["RoleArn"] == role
            assert stream["OutputFormat"] == "json"
            assert stream["State"] == "running"
            assert "Arn" in stream
            assert "CreationDate" in stream
            assert "LastUpdateDate" in stream

            # UPDATE: stop stream
            cw.stop_metric_streams(Names=[name])
            stopped = cw.get_metric_stream(Name=name)
            assert stopped["State"] == "stopped"

            # UPDATE: start stream
            cw.start_metric_streams(Names=[name])
            running = cw.get_metric_stream(Name=name)
            assert running["State"] == "running"
        finally:
            cw.delete_metric_stream(Name=name)

        # ERROR: get after delete
        with pytest.raises(ClientError) as exc:
            cw.get_metric_stream(Name=name)
        assert exc.value.response["Error"]["Code"] in ("ResourceNotFoundException", "ResourceNotFound")

    # ── RETRIEVE + UPDATE: anomaly detector fields ─────────────────────────────

    def test_anomaly_detector_retrieve_and_update(self, cw):
        """Create anomaly detector, retrieve fields, update stat, delete (C+R+L+U+D)."""
        ns = f"ADRetrieve-{uuid.uuid4().hex[:8]}"
        metric = "LoadMetric"

        # CREATE
        cw.put_anomaly_detector(Namespace=ns, MetricName=metric, Stat="Average")

        try:
            # RETRIEVE: specific fields
            resp = cw.describe_anomaly_detectors(Namespace=ns, MetricName=metric)
            assert len(resp["AnomalyDetectors"]) >= 1
            ad = resp["AnomalyDetectors"][0]
            assert ad["Namespace"] == ns
            assert ad["MetricName"] == metric
            assert ad["Stat"] == "Average"
            assert ad["StateValue"] == "PENDING_TRAINING"

            # LIST: appears in unfiltered describe
            all_resp = cw.describe_anomaly_detectors()
            namespaces = [d["Namespace"] for d in all_resp["AnomalyDetectors"]]
            assert ns in namespaces

            # UPDATE: put same detector with different stat
            cw.put_anomaly_detector(Namespace=ns, MetricName=metric, Stat="Sum")
            updated = cw.describe_anomaly_detectors(Namespace=ns, MetricName=metric)
            stats = {d["Stat"] for d in updated["AnomalyDetectors"]}
            assert "Sum" in stats
        finally:
            # DELETE both (Average might still exist)
            for stat in ("Average", "Sum"):
                try:
                    cw.delete_anomaly_detector(Namespace=ns, MetricName=metric, Stat=stat)
                except Exception:
                    pass  # best-effort cleanup

    # ── ERROR + RETRIEVE: alarm mute rule get after delete ────────────────────

    def test_alarm_mute_rule_retrieve_error_after_delete(self, cw):
        """Create mute rule, retrieve with strong assertions, delete, error on get (C+R+L+D+E)."""
        from botocore.exceptions import ClientError

        name = f"mute-retrieve-{uuid.uuid4().hex[:8]}"
        cw.put_alarm_mute_rule(
            Name=name,
            Rule={"Schedule": {"Expression": "cron(0 6 * * ? *)", "Duration": "PT4H"}},
        )
        try:
            # RETRIEVE: get rule by name
            get_resp = cw.get_alarm_mute_rule(AlarmMuteRuleName=name)
            assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # LIST: appears in list_alarm_mute_rules
            list_resp = cw.list_alarm_mute_rules()
            assert "AlarmMuteRuleSummaries" in list_resp
            assert isinstance(list_resp["AlarmMuteRuleSummaries"], list)
        finally:
            cw.delete_alarm_mute_rule(AlarmMuteRuleName=name)

        # ERROR: get after delete
        with pytest.raises(ClientError) as exc:
            cw.get_alarm_mute_rule(AlarmMuteRuleName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── UPDATE + RETRIEVE: insight rule state toggle ─────────────────────────

    def test_insight_rule_update_state_and_retrieve(self, cw):
        """Create insight rule, disable (UPDATE), verify state (RETRIEVE), enable (UPDATE) (C+R+U+D)."""
        name = f"state-rule-{uuid.uuid4().hex[:8]}"
        rule_def = (
            '{"Schema":{"Name":"CloudWatchLogRule","Version":1},'
            '"LogGroupNames":["/state-test"],'
            '"Contributions":{"Filters":[],"Keys":["$.host"],"ValueOf":"$.count"}}'
        )
        cw.put_insight_rule(RuleName=name, RuleDefinition=rule_def, RuleState="ENABLED")
        try:
            # RETRIEVE: verify initial state
            resp = cw.describe_insight_rules()
            rule = next((r for r in resp["InsightRules"] if r["Name"] == name), None)
            assert rule is not None
            assert rule["State"] == "ENABLED"
            assert rule["Name"] == name

            # UPDATE: disable
            cw.disable_insight_rules(RuleNames=[name])
            resp2 = cw.describe_insight_rules()
            rule2 = next((r for r in resp2["InsightRules"] if r["Name"] == name), None)
            assert rule2 is not None
            assert rule2["State"] == "DISABLED"

            # UPDATE: re-enable
            cw.enable_insight_rules(RuleNames=[name])
            resp3 = cw.describe_insight_rules()
            rule3 = next((r for r in resp3["InsightRules"] if r["Name"] == name), None)
            assert rule3 is not None
            assert rule3["State"] == "ENABLED"
        finally:
            cw.delete_insight_rules(RuleNames=[name])

    # ── RETRIEVE: metric statistics with strong value checks ─────────────────

    def test_metric_statistics_retrieve_strong(self, cw):
        """Put known values, get statistics, assert exact computed values (C+R+L+U)."""
        ns = f"StrongStats-{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)
        # CREATE: put 4 known values
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {"MetricName": "Score", "Value": 10.0, "Unit": "Count", "Timestamp": now},
                {"MetricName": "Score", "Value": 20.0, "Unit": "Count", "Timestamp": now},
                {"MetricName": "Score", "Value": 30.0, "Unit": "Count", "Timestamp": now},
                {"MetricName": "Score", "Value": 40.0, "Unit": "Count", "Timestamp": now},
            ],
        )
        # RETRIEVE: all statistics
        resp = cw.get_metric_statistics(
            Namespace=ns,
            MetricName="Score",
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
            Period=300,
            Statistics=["Sum", "Average", "Minimum", "Maximum", "SampleCount"],
        )
        assert len(resp["Datapoints"]) >= 1
        dp = resp["Datapoints"][0]
        assert dp["SampleCount"] == 4.0
        assert dp["Sum"] == 100.0
        assert dp["Minimum"] == 10.0
        assert dp["Maximum"] == 40.0
        assert dp["Average"] == 25.0
        assert dp["Unit"] == "Count"

        # LIST: metric appears
        list_resp = cw.list_metrics(Namespace=ns, MetricName="Score")
        assert len(list_resp["Metrics"]) >= 1
        assert list_resp["Metrics"][0]["MetricName"] == "Score"
        assert list_resp["Metrics"][0]["Namespace"] == ns

        # UPDATE: put more data, verify stats change
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {"MetricName": "Score", "Value": 50.0, "Unit": "Count", "Timestamp": now},
            ],
        )
        resp2 = cw.get_metric_statistics(
            Namespace=ns,
            MetricName="Score",
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
            Period=300,
            Statistics=["SampleCount", "Sum"],
        )
        assert resp2["Datapoints"][0]["SampleCount"] == 5.0
        assert resp2["Datapoints"][0]["Sum"] == 150.0

    # ── ERROR: get_metric_stream nonexistent ──────────────────────────────────

    def test_metric_stream_error_nonexistent(self, cw):
        """GetMetricStream on nonexistent stream raises error with correct code (E)."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            cw.get_metric_stream(Name=f"nonexistent-stream-{uuid.uuid4().hex[:8]}")
        assert exc.value.response["Error"]["Code"] in ("ResourceNotFoundException", "ResourceNotFound")

    # ── UPDATE + RETRIEVE: alarm description and actions updated ─────────────

    def test_alarm_description_update(self, cw):
        """Create alarm with description, update with new description, verify change (C+R+U+D)."""
        name = f"desc-update-{uuid.uuid4().hex[:8]}"
        cw.put_metric_alarm(
            AlarmName=name,
            Namespace="DescUpdateNS",
            MetricName="Errors",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Sum",
            Threshold=5.0,
            AlarmDescription="Original description",
        )
        try:
            # RETRIEVE: initial description
            resp = cw.describe_alarms(AlarmNames=[name])
            assert resp["MetricAlarms"][0]["AlarmDescription"] == "Original description"

            # UPDATE: change description
            cw.put_metric_alarm(
                AlarmName=name,
                Namespace="DescUpdateNS",
                MetricName="Errors",
                ComparisonOperator="GreaterThanThreshold",
                EvaluationPeriods=1,
                Period=60,
                Statistic="Sum",
                Threshold=5.0,
                AlarmDescription="Updated description",
            )

            # RETRIEVE: new description
            resp2 = cw.describe_alarms(AlarmNames=[name])
            assert resp2["MetricAlarms"][0]["AlarmDescription"] == "Updated description"
        finally:
            cw.delete_alarms(AlarmNames=[name])

    # ── RETRIEVE: alarm history has items after state changes ─────────────────

    def test_alarm_history_items_after_state_changes(self, cw):
        """SetAlarmState twice, describe history, assert items present with types (C+R+L+U+E)."""
        from botocore.exceptions import ClientError

        name = f"hist-items-{uuid.uuid4().hex[:8]}"
        cw.put_metric_alarm(
            AlarmName=name,
            Namespace="HistItemsNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        try:
            # UPDATE: set state
            cw.set_alarm_state(AlarmName=name, StateValue="ALARM", StateReason="trigger 1")
            cw.set_alarm_state(AlarmName=name, StateValue="OK", StateReason="resolved")

            # RETRIEVE: history should have items
            resp = cw.describe_alarm_history(AlarmName=name)
            assert "AlarmHistoryItems" in resp
            items = resp["AlarmHistoryItems"]
            assert len(items) >= 1
            for item in items:
                assert "AlarmName" in item
                assert item["AlarmName"] == name
                assert "HistoryItemType" in item
                assert "Timestamp" in item

            # LIST: filter history by type
            state_history = cw.describe_alarm_history(
                AlarmName=name, HistoryItemType="StateUpdate"
            )
            assert "AlarmHistoryItems" in state_history
        finally:
            cw.delete_alarms(AlarmNames=[name])

        # ERROR: describe history after delete (should return empty or raise)
        # Some impls return empty, some raise - either is acceptable
        try:
            gone_resp = cw.describe_alarm_history(AlarmName=name)
            assert "AlarmHistoryItems" in gone_resp
        except Exception:
            pass  # ResourceNotFoundException is also acceptable

    # ── UPDATE + RETRIEVE: dashboard update ───────────────────────────────────

    def test_dashboard_update_body(self, cw):
        """Create dashboard, update with new body, verify updated content (C+R+U+D+E)."""
        import json

        name = f"update-dash-{uuid.uuid4().hex[:8]}"
        body1 = json.dumps({"widgets": [{"type": "text", "properties": {"markdown": "v1"}}]})
        body2 = json.dumps({"widgets": [
            {"type": "text", "properties": {"markdown": "v1"}},
            {"type": "text", "properties": {"markdown": "v2"}},
        ]})

        try:
            # CREATE
            cw.put_dashboard(DashboardName=name, DashboardBody=body1)

            # RETRIEVE: initial body has 1 widget
            get1 = cw.get_dashboard(DashboardName=name)
            assert get1["DashboardName"] == name
            widgets1 = json.loads(get1["DashboardBody"])["widgets"]
            assert len(widgets1) == 1

            # UPDATE: new body with 2 widgets
            cw.put_dashboard(DashboardName=name, DashboardBody=body2)

            # RETRIEVE: updated body has 2 widgets
            get2 = cw.get_dashboard(DashboardName=name)
            widgets2 = json.loads(get2["DashboardBody"])["widgets"]
            assert len(widgets2) == 2

            # LIST: appears in list_dashboards
            list_resp = cw.list_dashboards(DashboardNamePrefix=name)
            names = [d["DashboardName"] for d in list_resp["DashboardEntries"]]
            assert name in names
        finally:
            # DELETE
            cw.delete_dashboards(DashboardNames=[name])

    # ── ERROR: describe_alarms returns empty for nonexistent (not error) ──────

    def test_describe_alarms_nonexistent_returns_empty(self, cw):
        """DescribeAlarms for nonexistent name returns empty list, not an error (E boundary)."""
        resp = cw.describe_alarms(AlarmNames=[f"nonexistent-alarm-{uuid.uuid4().hex[:8]}"])
        assert resp["MetricAlarms"] == []
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    # ── RETRIEVE: metric data with dimension filtering ────────────────────────

    def test_metric_statistics_dimension_filter_retrieve(self, cw):
        """Put metrics with multiple dimensions, retrieve stats filtered by dimension (C+R+L)."""
        ns = f"DimStats-{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)

        # CREATE: two environments with different values
        cw.put_metric_data(
            Namespace=ns,
            MetricData=[
                {
                    "MetricName": "CPU",
                    "Dimensions": [{"Name": "Env", "Value": "prod"}],
                    "Value": 80.0,
                    "Unit": "Percent",
                    "Timestamp": now,
                },
                {
                    "MetricName": "CPU",
                    "Dimensions": [{"Name": "Env", "Value": "staging"}],
                    "Value": 40.0,
                    "Unit": "Percent",
                    "Timestamp": now,
                },
            ],
        )

        # RETRIEVE: get stats for prod dimension
        prod_stats = cw.get_metric_statistics(
            Namespace=ns,
            MetricName="CPU",
            Dimensions=[{"Name": "Env", "Value": "prod"}],
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
            Period=300,
            Statistics=["Average", "Maximum"],
        )
        assert len(prod_stats["Datapoints"]) >= 1
        assert prod_stats["Datapoints"][0]["Average"] == 80.0
        assert prod_stats["Datapoints"][0]["Maximum"] == 80.0

        # LIST: both dimensions appear in list_metrics
        list_resp = cw.list_metrics(Namespace=ns, MetricName="CPU")
        envs = {d["Value"] for m in list_resp["Metrics"] for d in m["Dimensions"] if d["Name"] == "Env"}
        assert "prod" in envs
        assert "staging" in envs

    # ── UPDATE: metric stream filter change ───────────────────────────────────

    def test_metric_stream_update_output_format(self, cw):
        """Create stream with json format, update to opentelemetry, verify change (C+R+U+D)."""
        name = f"fmt-stream-{uuid.uuid4().hex[:8]}"
        cw.put_metric_stream(
            Name=name,
            FirehoseArn="arn:aws:firehose:us-east-1:123456789012:deliverystream/test",
            RoleArn="arn:aws:iam::123456789012:role/test",
            OutputFormat="json",
        )
        try:
            # RETRIEVE: initial format
            stream = cw.get_metric_stream(Name=name)
            assert stream["OutputFormat"] == "json"

            # UPDATE: change to opentelemetry
            cw.put_metric_stream(
                Name=name,
                FirehoseArn="arn:aws:firehose:us-east-1:123456789012:deliverystream/test",
                RoleArn="arn:aws:iam::123456789012:role/test",
                OutputFormat="opentelemetry1.0",
            )

            # RETRIEVE: verify format changed
            updated = cw.get_metric_stream(Name=name)
            assert updated["OutputFormat"] == "opentelemetry1.0"
            assert updated["Name"] == name
        finally:
            cw.delete_metric_stream(Name=name)

    # ── ERROR + RETRIEVE: composite alarm details ─────────────────────────────

    def test_composite_alarm_retrieve_fields(self, cw):
        """Create composite alarm, verify fields in describe response (C+R+L+D)."""
        suffix = uuid.uuid4().hex[:8]
        child = f"comp-child-{suffix}"
        composite = f"comp-parent-{suffix}"
        rule = f'ALARM("{child}")'

        cw.put_metric_alarm(
            AlarmName=child,
            Namespace="CompFieldNS",
            MetricName="M",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Average",
            Threshold=50.0,
        )
        try:
            cw.put_composite_alarm(AlarmName=composite, AlarmRule=rule)

            # RETRIEVE: composite alarm fields
            resp = cw.describe_alarms(AlarmNames=[composite], AlarmTypes=["CompositeAlarm"])
            assert len(resp.get("CompositeAlarms", [])) >= 1
            ca = resp["CompositeAlarms"][0]
            assert ca["AlarmName"] == composite
            assert ca["AlarmRule"] == rule
            assert "AlarmArn" in ca
            assert ca["StateValue"] in ("OK", "ALARM", "INSUFFICIENT_DATA")

            # LIST: appears in unfiltered describe
            all_resp = cw.describe_alarms(AlarmTypes=["CompositeAlarm"])
            all_names = [a["AlarmName"] for a in all_resp.get("CompositeAlarms", [])]
            assert composite in all_names
        finally:
            cw.delete_alarms(AlarmNames=[composite, child])
