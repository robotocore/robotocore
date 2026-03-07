"""EventBridge Scheduler compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def scheduler():
    return make_client("scheduler")


class TestSchedulerOperations:
    def test_create_schedule(self, scheduler):
        response = scheduler.create_schedule(
            Name="test-schedule",
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:test-queue",
                "RoleArn": "arn:aws:iam::123456789012:role/scheduler-role",
            },
        )
        assert "ScheduleArn" in response
        scheduler.delete_schedule(Name="test-schedule")

    def test_get_schedule(self, scheduler):
        scheduler.create_schedule(
            Name="get-schedule",
            ScheduleExpression="rate(5 minutes)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        response = scheduler.get_schedule(Name="get-schedule")
        assert response["Name"] == "get-schedule"
        scheduler.delete_schedule(Name="get-schedule")

    def test_list_schedules(self, scheduler):
        scheduler.create_schedule(
            Name="list-schedule",
            ScheduleExpression="rate(1 day)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        response = scheduler.list_schedules()
        names = [s["Name"] for s in response["Schedules"]]
        assert "list-schedule" in names
        scheduler.delete_schedule(Name="list-schedule")

    def test_delete_schedule(self, scheduler):
        scheduler.create_schedule(
            Name="del-schedule",
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        scheduler.delete_schedule(Name="del-schedule")
        response = scheduler.list_schedules()
        names = [s["Name"] for s in response["Schedules"]]
        assert "del-schedule" not in names

    def test_create_schedule_group(self, scheduler):
        response = scheduler.create_schedule_group(Name="test-group")
        assert "ScheduleGroupArn" in response
        scheduler.delete_schedule_group(Name="test-group")

    def test_update_schedule(self, scheduler):
        """Update an existing schedule."""
        scheduler.create_schedule(
            Name="update-schedule",
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        scheduler.update_schedule(
            Name="update-schedule",
            ScheduleExpression="rate(2 hours)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        response = scheduler.get_schedule(Name="update-schedule")
        assert response["ScheduleExpression"] == "rate(2 hours)"
        scheduler.delete_schedule(Name="update-schedule")


class TestSchedulerGroups:
    def test_get_schedule_group(self, scheduler):
        """Get a schedule group by name."""
        scheduler.create_schedule_group(Name="get-group")
        response = scheduler.get_schedule_group(Name="get-group")
        assert response["Name"] == "get-group"
        assert response["State"] == "ACTIVE"
        scheduler.delete_schedule_group(Name="get-group")

    def test_list_schedule_groups(self, scheduler):
        """List schedule groups includes default group."""
        response = scheduler.list_schedule_groups()
        names = [g["Name"] for g in response["ScheduleGroups"]]
        assert "default" in names

    def test_list_schedule_groups_with_custom(self, scheduler):
        """Custom groups appear in list."""
        scheduler.create_schedule_group(Name="custom-group")
        response = scheduler.list_schedule_groups()
        names = [g["Name"] for g in response["ScheduleGroups"]]
        assert "custom-group" in names
        scheduler.delete_schedule_group(Name="custom-group")

    def test_delete_schedule_group(self, scheduler):
        """Delete a custom schedule group."""
        scheduler.create_schedule_group(Name="del-group")
        scheduler.delete_schedule_group(Name="del-group")
        response = scheduler.list_schedule_groups()
        names = [g["Name"] for g in response["ScheduleGroups"]]
        assert "del-group" not in names


class TestSchedulerWithGroup:
    def test_schedule_in_custom_group(self, scheduler):
        """Create a schedule in a custom group."""
        scheduler.create_schedule_group(Name="sched-group")
        try:
            scheduler.create_schedule(
                Name="grouped-schedule",
                GroupName="sched-group",
                ScheduleExpression="rate(1 hour)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={
                    "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                    "RoleArn": "arn:aws:iam::123456789012:role/role",
                },
            )
            response = scheduler.get_schedule(Name="grouped-schedule", GroupName="sched-group")
            assert response["Name"] == "grouped-schedule"
            assert response["GroupName"] == "sched-group"
            scheduler.delete_schedule(Name="grouped-schedule", GroupName="sched-group")
        finally:
            scheduler.delete_schedule_group(Name="sched-group")
