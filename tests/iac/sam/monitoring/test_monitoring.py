"""IaC test: sam - monitoring."""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.stack_deployer import delete_stack, deploy_and_yield, get_stack_outputs

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    template = (Path(__file__).parent / "template.yaml").read_text()
    stack_name = f"{test_run_id}-sam-monitoring"
    stack = deploy_and_yield(stack_name, template)
    yield stack
    delete_stack(stack_name)


class TestMonitoring:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_topic_exists(self, deployed_stack, ensure_server):
        outputs = get_stack_outputs(deployed_stack)
        topic_arn = outputs.get("TopicArn")
        assert topic_arn is not None, "TopicArn output missing"

        sns = make_client("sns")
        resp = sns.get_topic_attributes(TopicArn=topic_arn)
        assert resp["Attributes"]["TopicArn"] == topic_arn

    def test_log_group_exists(self, deployed_stack, ensure_server):
        outputs = get_stack_outputs(deployed_stack)
        log_group_name = outputs.get("LogGroupName")
        assert log_group_name is not None, "LogGroupName output missing"

        logs = make_client("logs")
        resp = logs.describe_log_groups(logGroupNamePrefix=log_group_name)
        groups = resp["logGroups"]
        matching = [g for g in groups if g["logGroupName"] == log_group_name]
        assert len(matching) == 1
        assert matching[0]["retentionInDays"] == 7

    def test_alarm_exists(self, deployed_stack, ensure_server):
        outputs = get_stack_outputs(deployed_stack)
        alarm_name = outputs.get("AlarmName")
        assert alarm_name is not None, "AlarmName output missing"

        cw = make_client("cloudwatch")
        resp = cw.describe_alarms(AlarmNames=[alarm_name])
        alarms = resp["MetricAlarms"]
        assert len(alarms) == 1
        assert alarms[0]["AlarmName"] == alarm_name
        assert alarms[0]["Threshold"] == 10.0
        assert alarms[0]["ComparisonOperator"] == "GreaterThanThreshold"
