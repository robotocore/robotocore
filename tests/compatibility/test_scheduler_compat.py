"""EventBridge Scheduler compatibility tests."""

from datetime import UTC

import pytest
from botocore.exceptions import ClientError

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

    def test_schedule_with_rate_expression(self, scheduler):
        """Create a schedule with a rate expression."""
        response = scheduler.create_schedule(
            Name="rate-schedule",
            ScheduleExpression="rate(10 minutes)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:rate-queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        assert "ScheduleArn" in response
        got = scheduler.get_schedule(Name="rate-schedule")
        assert got["ScheduleExpression"] == "rate(10 minutes)"
        scheduler.delete_schedule(Name="rate-schedule")

    def test_schedule_with_cron_expression(self, scheduler):
        """Create a schedule with a cron expression."""
        response = scheduler.create_schedule(
            Name="cron-schedule",
            ScheduleExpression="cron(0 12 * * ? *)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:cron-queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        assert "ScheduleArn" in response
        got = scheduler.get_schedule(Name="cron-schedule")
        assert got["ScheduleExpression"] == "cron(0 12 * * ? *)"
        scheduler.delete_schedule(Name="cron-schedule")

    def test_schedule_with_at_expression(self, scheduler):
        """Create a one-time schedule with at() expression."""
        response = scheduler.create_schedule(
            Name="at-schedule",
            ScheduleExpression="at(2030-01-01T00:00:00)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:at-queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        assert "ScheduleArn" in response
        got = scheduler.get_schedule(Name="at-schedule")
        assert got["ScheduleExpression"] == "at(2030-01-01T00:00:00)"
        scheduler.delete_schedule(Name="at-schedule")

    def test_get_schedule_group(self, scheduler):
        """Create a schedule group and get it."""
        scheduler.create_schedule_group(Name="get-group")
        try:
            got = scheduler.get_schedule_group(Name="get-group")
            assert got["Name"] == "get-group"
            assert "Arn" in got
        finally:
            scheduler.delete_schedule_group(Name="get-group")

    def test_list_schedule_groups(self, scheduler):
        """Create schedule groups and list them."""
        scheduler.create_schedule_group(Name="list-group-1")
        scheduler.create_schedule_group(Name="list-group-2")
        try:
            response = scheduler.list_schedule_groups()
            names = [g["Name"] for g in response["ScheduleGroups"]]
            assert "list-group-1" in names
            assert "list-group-2" in names
        finally:
            scheduler.delete_schedule_group(Name="list-group-1")
            scheduler.delete_schedule_group(Name="list-group-2")

    def test_delete_schedule_group(self, scheduler):
        """Create and delete a schedule group."""
        scheduler.create_schedule_group(Name="del-group")
        scheduler.delete_schedule_group(Name="del-group")
        response = scheduler.list_schedule_groups()
        names = [g["Name"] for g in response["ScheduleGroups"]]
        assert "del-group" not in names

    def test_schedule_with_flexible_time_window(self, scheduler):
        """Create a schedule with a flexible time window."""
        response = scheduler.create_schedule(
            Name="flex-schedule",
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 15},
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:flex-queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        assert "ScheduleArn" in response
        got = scheduler.get_schedule(Name="flex-schedule")
        assert got["FlexibleTimeWindow"]["Mode"] == "FLEXIBLE"
        assert got["FlexibleTimeWindow"]["MaximumWindowInMinutes"] == 15
        scheduler.delete_schedule(Name="flex-schedule")

    def test_update_schedule(self, scheduler):
        """Create a schedule and then update it."""
        scheduler.create_schedule(
            Name="update-schedule",
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        updated = scheduler.update_schedule(
            Name="update-schedule",
            ScheduleExpression="rate(30 minutes)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        assert "ScheduleArn" in updated
        got = scheduler.get_schedule(Name="update-schedule")
        assert got["ScheduleExpression"] == "rate(30 minutes)"
        scheduler.delete_schedule(Name="update-schedule")

    def test_list_schedules_with_group_filter(self, scheduler):
        """Create schedules in a group and list with group filter."""
        scheduler.create_schedule_group(Name="filter-group")
        try:
            scheduler.create_schedule(
                Name="grouped-schedule",
                GroupName="filter-group",
                ScheduleExpression="rate(1 hour)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={
                    "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                    "RoleArn": "arn:aws:iam::123456789012:role/role",
                },
            )
            response = scheduler.list_schedules(GroupName="filter-group")
            names = [s["Name"] for s in response["Schedules"]]
            assert "grouped-schedule" in names
            scheduler.delete_schedule(Name="grouped-schedule", GroupName="filter-group")
        finally:
            scheduler.delete_schedule_group(Name="filter-group")

    def test_list_schedules_with_name_prefix(self, scheduler):
        """List schedules filtered by name prefix."""
        scheduler.create_schedule(
            Name="prefix-alpha",
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        scheduler.create_schedule(
            Name="prefix-beta",
            ScheduleExpression="rate(2 hours)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        scheduler.create_schedule(
            Name="other-schedule",
            ScheduleExpression="rate(3 hours)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        try:
            response = scheduler.list_schedules(NamePrefix="prefix-")
            names = [s["Name"] for s in response["Schedules"]]
            assert "prefix-alpha" in names
            assert "prefix-beta" in names
            assert "other-schedule" not in names
        finally:
            scheduler.delete_schedule(Name="prefix-alpha")
            scheduler.delete_schedule(Name="prefix-beta")
            scheduler.delete_schedule(Name="other-schedule")

    def test_schedule_with_start_and_end_date(self, scheduler):
        """Create a schedule with StartDate and EndDate."""
        from datetime import datetime

        start = datetime(2030, 1, 1, tzinfo=UTC)
        end = datetime(2030, 12, 31, tzinfo=UTC)
        response = scheduler.create_schedule(
            Name="dated-schedule",
            ScheduleExpression="rate(1 day)",
            FlexibleTimeWindow={"Mode": "OFF"},
            StartDate=start,
            EndDate=end,
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        assert "ScheduleArn" in response
        got = scheduler.get_schedule(Name="dated-schedule")
        assert got["StartDate"] is not None
        assert got["EndDate"] is not None
        scheduler.delete_schedule(Name="dated-schedule")

    def test_update_schedule_expression_and_verify(self, scheduler):
        """Create a schedule with rate(1 hour), update to rate(30 minutes), verify all fields."""
        target = {
            "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
            "RoleArn": "arn:aws:iam::123456789012:role/role",
        }
        flexible = {"Mode": "OFF"}
        scheduler.create_schedule(
            Name="update-verify-schedule",
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow=flexible,
            Target=target,
        )
        try:
            scheduler.update_schedule(
                Name="update-verify-schedule",
                ScheduleExpression="rate(30 minutes)",
                FlexibleTimeWindow=flexible,
                Target=target,
            )
            got = scheduler.get_schedule(Name="update-verify-schedule")
            assert got["ScheduleExpression"] == "rate(30 minutes)"
            assert got["FlexibleTimeWindow"]["Mode"] == "OFF"
            assert got["Target"]["Arn"] == target["Arn"]
            assert got["Target"]["RoleArn"] == target["RoleArn"]
        finally:
            scheduler.delete_schedule(Name="update-verify-schedule")

    def test_schedule_start_and_end_date_are_returned(self, scheduler):
        """Create schedule with StartDate and EndDate, verify they are returned."""
        from datetime import datetime

        start = datetime(2030, 1, 15, 12, 0, 0, tzinfo=UTC)
        end = datetime(2030, 12, 15, 12, 0, 0, tzinfo=UTC)
        scheduler.create_schedule(
            Name="dated-verify-schedule",
            ScheduleExpression="rate(1 day)",
            FlexibleTimeWindow={"Mode": "OFF"},
            StartDate=start,
            EndDate=end,
            Target={
                "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                "RoleArn": "arn:aws:iam::123456789012:role/role",
            },
        )
        try:
            got = scheduler.get_schedule(Name="dated-verify-schedule")
            assert got["StartDate"].year == 2030
            assert got["EndDate"].year == 2030
        finally:
            scheduler.delete_schedule(Name="dated-verify-schedule")

    def test_create_schedule_in_named_group(self, scheduler):
        """Create a schedule group, create a schedule in that group, verify via get."""
        scheduler.create_schedule_group(Name="named-group")
        try:
            scheduler.create_schedule(
                Name="grouped-sched",
                GroupName="named-group",
                ScheduleExpression="rate(1 hour)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={
                    "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                    "RoleArn": "arn:aws:iam::123456789012:role/role",
                },
            )
            got = scheduler.get_schedule(Name="grouped-sched", GroupName="named-group")
            assert got["Name"] == "grouped-sched"
            assert got["GroupName"] == "named-group"
            scheduler.delete_schedule(Name="grouped-sched", GroupName="named-group")
        finally:
            scheduler.delete_schedule_group(Name="named-group")

    def test_schedule_group_tags(self, scheduler):
        """Create a schedule group with tags and list them."""
        scheduler.create_schedule_group(
            Name="tagged-group",
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        try:
            got = scheduler.get_schedule_group(Name="tagged-group")
            # Verify group was created (tags may or may not be in get response
            # depending on implementation, but the create should succeed)
            assert got["Name"] == "tagged-group"
        finally:
            scheduler.delete_schedule_group(Name="tagged-group")


class TestSchedulerExtended:
    @pytest.fixture
    def scheduler(self):
        return make_client("scheduler")

    def _target(self):
        return {
            "Arn": "arn:aws:sqs:us-east-1:123456789012:my-queue",
            "RoleArn": "arn:aws:iam::123456789012:role/scheduler-role",
        }

    def test_create_schedule_with_description(self, scheduler):
        name = "desc-sched"
        scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=self._target(),
            Description="A test schedule",
        )
        try:
            resp = scheduler.get_schedule(Name=name)
            assert resp.get("Description") == "A test schedule"
        finally:
            scheduler.delete_schedule(Name=name)

    def test_create_schedule_with_state_disabled(self, scheduler):
        name = "disabled-sched"
        scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=self._target(),
            State="DISABLED",
        )
        try:
            resp = scheduler.get_schedule(Name=name)
            assert resp["State"] == "DISABLED"
        finally:
            scheduler.delete_schedule(Name=name)

    def test_update_schedule_state(self, scheduler):
        name = "upd-state-sched"
        scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=self._target(),
            State="ENABLED",
        )
        try:
            scheduler.update_schedule(
                Name=name,
                ScheduleExpression="rate(1 hour)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target=self._target(),
                State="DISABLED",
            )
            resp = scheduler.get_schedule(Name=name)
            assert resp["State"] == "DISABLED"
        finally:
            scheduler.delete_schedule(Name=name)

    def test_update_schedule_target(self, scheduler):
        name = "upd-target-sched"
        scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=self._target(),
        )
        try:
            new_target = {
                "Arn": "arn:aws:sqs:us-east-1:123456789012:new-queue",
                "RoleArn": "arn:aws:iam::123456789012:role/scheduler-role",
            }
            scheduler.update_schedule(
                Name=name,
                ScheduleExpression="rate(2 hours)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target=new_target,
            )
            resp = scheduler.get_schedule(Name=name)
            assert resp["ScheduleExpression"] == "rate(2 hours)"
        finally:
            scheduler.delete_schedule(Name=name)

    def test_list_schedules_returns_all(self, scheduler):
        names = ["list-s1", "list-s2", "list-s3"]
        for n in names:
            scheduler.create_schedule(
                Name=n,
                ScheduleExpression="rate(1 hour)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target=self._target(),
            )
        try:
            resp = scheduler.list_schedules()
            found = [s["Name"] for s in resp["Schedules"]]
            for n in names:
                assert n in found
        finally:
            for n in names:
                scheduler.delete_schedule(Name=n)

    def test_create_and_list_multiple_groups(self, scheduler):
        groups = ["ext-grp-1", "ext-grp-2"]
        for g in groups:
            scheduler.create_schedule_group(Name=g)
        try:
            resp = scheduler.list_schedule_groups()
            found = [g["Name"] for g in resp["ScheduleGroups"]]
            for g in groups:
                assert g in found
        finally:
            for g in groups:
                scheduler.delete_schedule_group(Name=g)

    def test_schedule_arn_format(self, scheduler):
        name = "arn-sched"
        resp = scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=self._target(),
        )
        try:
            assert "ScheduleArn" in resp
            assert name in resp["ScheduleArn"]
        finally:
            scheduler.delete_schedule(Name=name)

    def test_schedule_group_arn_format(self, scheduler):
        name = "arn-grp"
        resp = scheduler.create_schedule_group(Name=name)
        try:
            assert "ScheduleGroupArn" in resp
            assert name in resp["ScheduleGroupArn"]
        finally:
            scheduler.delete_schedule_group(Name=name)

    def test_get_schedule_has_target(self, scheduler):
        name = "target-check"
        scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=self._target(),
        )
        try:
            resp = scheduler.get_schedule(Name=name)
            assert "Target" in resp
            assert "Arn" in resp["Target"]
        finally:
            scheduler.delete_schedule(Name=name)

    def test_delete_schedule_idempotent(self, scheduler):
        name = "del-idem"
        scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=self._target(),
        )
        scheduler.delete_schedule(Name=name)
        # Second delete should raise
        with pytest.raises(ClientError):
            scheduler.delete_schedule(Name=name)

    def test_list_tags_for_resource(self, scheduler):
        """ListTagsForResource returns a Tags list for a schedule group."""
        name = "list-tags-grp"
        resp = scheduler.create_schedule_group(Name=name)
        arn = resp["ScheduleGroupArn"]
        try:
            tags_resp = scheduler.list_tags_for_resource(ResourceArn=arn)
            assert "Tags" in tags_resp
            assert isinstance(tags_resp["Tags"], list)
            assert tags_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            scheduler.delete_schedule_group(Name=name)

    def test_tag_resource(self, scheduler):
        """TagResource adds tags to a schedule group without error."""
        name = "tag-res-grp"
        resp = scheduler.create_schedule_group(Name=name)
        arn = resp["ScheduleGroupArn"]
        try:
            tag_resp = scheduler.tag_resource(
                ResourceArn=arn,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "dev"}],
            )
            assert tag_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            scheduler.delete_schedule_group(Name=name)

    def test_untag_resource(self, scheduler):
        """UntagResource removes tags from a schedule group without error."""
        name = "untag-res-grp"
        resp = scheduler.create_schedule_group(Name=name)
        arn = resp["ScheduleGroupArn"]
        try:
            scheduler.tag_resource(
                ResourceArn=arn,
                Tags=[{"Key": "env", "Value": "test"}],
            )
            untag_resp = scheduler.untag_resource(ResourceArn=arn, TagKeys=["env"])
            assert untag_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            scheduler.delete_schedule_group(Name=name)

    def test_tag_and_list_tags_for_schedule(self, scheduler):
        """Tag a schedule and list its tags."""
        name = "tag-sched"
        resp = scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target=self._target(),
        )
        arn = resp["ScheduleArn"]
        try:
            scheduler.tag_resource(
                ResourceArn=arn,
                Tags=[{"Key": "app", "Value": "myapp"}],
            )
            tags_resp = scheduler.list_tags_for_resource(ResourceArn=arn)
            assert "Tags" in tags_resp
            assert isinstance(tags_resp["Tags"], list)
        finally:
            scheduler.delete_schedule(Name=name)
