"""EventBridge Scheduler compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def scheduler():
    return make_client("scheduler")


def _sched_target():
    return {
        "Arn": "arn:aws:sqs:us-east-1:123456789012:test-queue",
        "RoleArn": "arn:aws:iam::123456789012:role/scheduler-role",
    }


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


class TestSchedulerExtended:
    """Extended Scheduler compatibility tests."""

    def test_list_schedules_with_name_prefix(self, scheduler):
        """ListSchedules with NamePrefix filter."""
        suffix = uuid.uuid4().hex[:8]
        names = [f"pfx-{suffix}-a", f"pfx-{suffix}-b", f"other-{suffix}"]
        for n in names:
            scheduler.create_schedule(
                Name=n,
                ScheduleExpression="rate(1 hour)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target=_sched_target(),
            )

        resp = scheduler.list_schedules(NamePrefix=f"pfx-{suffix}")
        matched = [s["Name"] for s in resp["Schedules"]]
        assert f"pfx-{suffix}-a" in matched
        assert f"pfx-{suffix}-b" in matched
        assert f"other-{suffix}" not in matched

        for n in names:
            scheduler.delete_schedule(Name=n)

    def test_schedule_with_rate_expression(self, scheduler):
        """Create schedule with rate expression and verify it's stored."""
        suffix = uuid.uuid4().hex[:8]
        name = f"rate-sched-{suffix}"
        scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(15 minutes)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=_sched_target(),
        )
        resp = scheduler.get_schedule(Name=name)
        assert resp["ScheduleExpression"] == "rate(15 minutes)"
        scheduler.delete_schedule(Name=name)

    def test_schedule_with_cron_expression(self, scheduler):
        """Create schedule with cron expression."""
        suffix = uuid.uuid4().hex[:8]
        name = f"cron-sched-{suffix}"
        scheduler.create_schedule(
            Name=name,
            ScheduleExpression="cron(0 8 * * ? *)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=_sched_target(),
        )
        resp = scheduler.get_schedule(Name=name)
        assert resp["ScheduleExpression"] == "cron(0 8 * * ? *)"
        scheduler.delete_schedule(Name=name)

    def test_schedule_with_flexible_time_window(self, scheduler):
        """Create schedule with flexible time window."""
        suffix = uuid.uuid4().hex[:8]
        name = f"flex-sched-{suffix}"
        scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 15},
            Target=_sched_target(),
        )
        resp = scheduler.get_schedule(Name=name)
        assert resp["FlexibleTimeWindow"]["Mode"] == "FLEXIBLE"
        assert resp["FlexibleTimeWindow"]["MaximumWindowInMinutes"] == 15
        scheduler.delete_schedule(Name=name)

    def test_create_schedule_in_non_default_group(self, scheduler):
        """Create a schedule in a custom group."""
        suffix = uuid.uuid4().hex[:8]
        group_name = f"custom-grp-{suffix}"
        sched_name = f"grp-sched-{suffix}"

        scheduler.create_schedule_group(Name=group_name)
        scheduler.create_schedule(
            Name=sched_name,
            GroupName=group_name,
            ScheduleExpression="rate(1 day)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=_sched_target(),
        )

        resp = scheduler.get_schedule(Name=sched_name)
        assert resp["Name"] == sched_name
        assert resp["GroupName"] == group_name

        scheduler.delete_schedule(Name=sched_name)
        scheduler.delete_schedule_group(Name=group_name)

    def test_update_schedule(self, scheduler):
        """Update a schedule's expression and verify."""
        suffix = uuid.uuid4().hex[:8]
        name = f"update-sched-{suffix}"
        scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=_sched_target(),
        )

        scheduler.update_schedule(
            Name=name,
            ScheduleExpression="rate(30 minutes)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=_sched_target(),
        )
        resp = scheduler.get_schedule(Name=name)
        assert resp["ScheduleExpression"] == "rate(30 minutes)"
        scheduler.delete_schedule(Name=name)

    def test_list_schedule_groups_with_name_prefix(self, scheduler):
        """ListScheduleGroups with NamePrefix filter."""
        suffix = uuid.uuid4().hex[:8]
        group_names = [f"grp-{suffix}-alpha", f"grp-{suffix}-beta", f"other-grp-{suffix}"]
        for gn in group_names:
            scheduler.create_schedule_group(Name=gn)

        resp = scheduler.list_schedule_groups(NamePrefix=f"grp-{suffix}")
        matched = [g["Name"] for g in resp["ScheduleGroups"]]
        assert f"grp-{suffix}-alpha" in matched
        assert f"grp-{suffix}-beta" in matched
        assert f"other-grp-{suffix}" not in matched

        for gn in group_names:
            scheduler.delete_schedule_group(Name=gn)

    def test_get_schedule_group(self, scheduler):
        """GetScheduleGroup returns expected fields."""
        suffix = uuid.uuid4().hex[:8]
        group_name = f"get-grp-{suffix}"
        scheduler.create_schedule_group(Name=group_name)

        resp = scheduler.get_schedule_group(Name=group_name)
        assert resp["Name"] == group_name
        assert "Arn" in resp
        assert resp["State"] == "ACTIVE"

        scheduler.delete_schedule_group(Name=group_name)

    def test_delete_schedule_group(self, scheduler):
        """Delete a schedule group and verify it's gone."""
        suffix = uuid.uuid4().hex[:8]
        group_name = f"del-grp-{suffix}"
        scheduler.create_schedule_group(Name=group_name)
        scheduler.delete_schedule_group(Name=group_name)

        resp = scheduler.list_schedule_groups()
        names = [g["Name"] for g in resp["ScheduleGroups"]]
        assert group_name not in names

    def test_schedule_arn_format(self, scheduler):
        """Schedule ARN includes region and name."""
        suffix = uuid.uuid4().hex[:8]
        name = f"arn-sched-{suffix}"
        resp = scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=_sched_target(),
        )
        arn = resp["ScheduleArn"]
        assert arn.startswith("arn:aws:scheduler:")
        assert name in arn
        scheduler.delete_schedule(Name=name)

    def test_schedule_group_arn_format(self, scheduler):
        """Schedule group ARN format is correct."""
        suffix = uuid.uuid4().hex[:8]
        group_name = f"arn-grp-{suffix}"
        resp = scheduler.create_schedule_group(Name=group_name)
        arn = resp["ScheduleGroupArn"]
        assert arn.startswith("arn:aws:scheduler:")
        assert group_name in arn
        scheduler.delete_schedule_group(Name=group_name)

    def test_get_schedule_not_found(self, scheduler):
        """GetSchedule for nonexistent schedule raises error."""
        import botocore.exceptions

        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            scheduler.get_schedule(Name="nonexistent-schedule-xyz")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_update_schedule_state(self, scheduler):
        """Update schedule to DISABLED state."""
        suffix = uuid.uuid4().hex[:8]
        name = f"state-sched-{suffix}"
        scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=_sched_target(),
            State="ENABLED",
        )

        scheduler.update_schedule(
            Name=name,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=_sched_target(),
            State="DISABLED",
        )
        resp = scheduler.get_schedule(Name=name)
        assert resp["State"] == "DISABLED"
        scheduler.delete_schedule(Name=name)

    def test_schedule_with_description(self, scheduler):
        """Create schedule with description."""
        suffix = uuid.uuid4().hex[:8]
        name = f"desc-sched-{suffix}"
        scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=_sched_target(),
            Description="My test schedule",
        )
        resp = scheduler.get_schedule(Name=name)
        assert resp["Description"] == "My test schedule"
        scheduler.delete_schedule(Name=name)

    def test_list_schedule_groups_includes_default(self, scheduler):
        """ListScheduleGroups always includes the default group."""
        resp = scheduler.list_schedule_groups()
        names = [g["Name"] for g in resp["ScheduleGroups"]]
        assert "default" in names

    def test_create_schedule_default_group(self, scheduler):
        """Schedule created without GroupName defaults to 'default'."""
        suffix = uuid.uuid4().hex[:8]
        name = f"default-grp-{suffix}"
        scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=_sched_target(),
        )
        resp = scheduler.get_schedule(Name=name)
        assert resp["GroupName"] == "default"
        scheduler.delete_schedule(Name=name)

    def test_delete_schedule_not_found(self, scheduler):
        """Deleting a nonexistent schedule raises error."""
        import botocore.exceptions

        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            scheduler.delete_schedule(Name="nonexistent-schedule-xyz")
        assert "ResourceNotFoundException" in str(exc_info.value)
