"""Compatibility tests for AWS Budgets service."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client

ACCOUNT_ID = "123456789012"


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def budgets():
    return make_client("budgets")


class TestBudgetOperations:
    """Tests for core budget CRUD operations."""

    def test_create_and_describe_budget(self, budgets):
        name = _unique("budget")
        budgets.create_budget(
            AccountId=ACCOUNT_ID,
            Budget={
                "BudgetName": name,
                "BudgetLimit": {"Amount": "100", "Unit": "USD"},
                "TimeUnit": "MONTHLY",
                "BudgetType": "COST",
            },
        )
        try:
            resp = budgets.describe_budget(AccountId=ACCOUNT_ID, BudgetName=name)
            budget = resp["Budget"]
            assert budget["BudgetName"] == name
            assert budget["BudgetLimit"]["Amount"] == "100"
            assert budget["BudgetLimit"]["Unit"] == "USD"
            assert budget["TimeUnit"] == "MONTHLY"
            assert budget["BudgetType"] == "COST"
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)

    def test_describe_budgets_lists_all(self, budgets):
        names = [_unique("budget") for _ in range(3)]
        for name in names:
            budgets.create_budget(
                AccountId=ACCOUNT_ID,
                Budget={
                    "BudgetName": name,
                    "BudgetLimit": {"Amount": "50", "Unit": "USD"},
                    "TimeUnit": "MONTHLY",
                    "BudgetType": "COST",
                },
            )
        try:
            resp = budgets.describe_budgets(AccountId=ACCOUNT_ID)
            returned_names = {b["BudgetName"] for b in resp["Budgets"]}
            for name in names:
                assert name in returned_names
        finally:
            for name in names:
                budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)

    def test_delete_budget(self, budgets):
        name = _unique("budget")
        budgets.create_budget(
            AccountId=ACCOUNT_ID,
            Budget={
                "BudgetName": name,
                "BudgetLimit": {"Amount": "100", "Unit": "USD"},
                "TimeUnit": "MONTHLY",
                "BudgetType": "COST",
            },
        )
        budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)

        with pytest.raises(ClientError) as exc:
            budgets.describe_budget(AccountId=ACCOUNT_ID, BudgetName=name)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_describe_nonexistent_budget_raises(self, budgets):
        with pytest.raises(ClientError) as exc:
            budgets.describe_budget(AccountId=ACCOUNT_ID, BudgetName="nonexistent-budget-xyz")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_delete_nonexistent_budget_raises(self, budgets):
        with pytest.raises(ClientError) as exc:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName="nonexistent-budget-xyz")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_create_duplicate_budget_raises(self, budgets):
        name = _unique("budget")
        budgets.create_budget(
            AccountId=ACCOUNT_ID,
            Budget={
                "BudgetName": name,
                "BudgetLimit": {"Amount": "100", "Unit": "USD"},
                "TimeUnit": "MONTHLY",
                "BudgetType": "COST",
            },
        )
        try:
            with pytest.raises(ClientError) as exc:
                budgets.create_budget(
                    AccountId=ACCOUNT_ID,
                    Budget={
                        "BudgetName": name,
                        "BudgetLimit": {"Amount": "100", "Unit": "USD"},
                        "TimeUnit": "MONTHLY",
                        "BudgetType": "COST",
                    },
                )
            assert exc.value.response["Error"]["Code"] == "DuplicateRecordException"
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)


class TestBudgetNotifications:
    """Tests for budget notification operations."""

    def test_create_and_describe_notification(self, budgets):
        name = _unique("budget")
        budgets.create_budget(
            AccountId=ACCOUNT_ID,
            Budget={
                "BudgetName": name,
                "BudgetLimit": {"Amount": "100", "Unit": "USD"},
                "TimeUnit": "MONTHLY",
                "BudgetType": "COST",
            },
        )
        try:
            budgets.create_notification(
                AccountId=ACCOUNT_ID,
                BudgetName=name,
                Notification={
                    "NotificationType": "ACTUAL",
                    "ComparisonOperator": "GREATER_THAN",
                    "Threshold": 80.0,
                    "ThresholdType": "PERCENTAGE",
                },
                Subscribers=[
                    {"SubscriptionType": "EMAIL", "Address": "test@example.com"},
                ],
            )
            resp = budgets.describe_notifications_for_budget(AccountId=ACCOUNT_ID, BudgetName=name)
            notifications = resp["Notifications"]
            assert len(notifications) >= 1
            notif = notifications[0]
            assert notif["NotificationType"] == "ACTUAL"
            assert notif["ComparisonOperator"] == "GREATER_THAN"
            assert notif["Threshold"] == 80.0
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)

    def test_delete_notification(self, budgets):
        name = _unique("budget")
        budgets.create_budget(
            AccountId=ACCOUNT_ID,
            Budget={
                "BudgetName": name,
                "BudgetLimit": {"Amount": "100", "Unit": "USD"},
                "TimeUnit": "MONTHLY",
                "BudgetType": "COST",
            },
        )
        try:
            notification = {
                "NotificationType": "ACTUAL",
                "ComparisonOperator": "GREATER_THAN",
                "Threshold": 80.0,
                "ThresholdType": "PERCENTAGE",
            }
            budgets.create_notification(
                AccountId=ACCOUNT_ID,
                BudgetName=name,
                Notification=notification,
                Subscribers=[
                    {"SubscriptionType": "EMAIL", "Address": "test@example.com"},
                ],
            )
            budgets.delete_notification(
                AccountId=ACCOUNT_ID,
                BudgetName=name,
                Notification=notification,
            )
            resp = budgets.describe_notifications_for_budget(AccountId=ACCOUNT_ID, BudgetName=name)
            assert len(resp["Notifications"]) == 0
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)

    def test_describe_notifications_empty(self, budgets):
        name = _unique("budget")
        budgets.create_budget(
            AccountId=ACCOUNT_ID,
            Budget={
                "BudgetName": name,
                "BudgetLimit": {"Amount": "100", "Unit": "USD"},
                "TimeUnit": "MONTHLY",
                "BudgetType": "COST",
            },
        )
        try:
            resp = budgets.describe_notifications_for_budget(AccountId=ACCOUNT_ID, BudgetName=name)
            assert resp["Notifications"] == []
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)


class TestBudgetUpdateOperations:
    """Tests for UpdateBudget."""

    def test_update_budget(self, budgets):
        name = _unique("budget")
        budgets.create_budget(
            AccountId=ACCOUNT_ID,
            Budget={
                "BudgetName": name,
                "BudgetLimit": {"Amount": "100", "Unit": "USD"},
                "TimeUnit": "MONTHLY",
                "BudgetType": "COST",
            },
        )
        try:
            budgets.update_budget(
                AccountId=ACCOUNT_ID,
                NewBudget={
                    "BudgetName": name,
                    "BudgetLimit": {"Amount": "200", "Unit": "USD"},
                    "TimeUnit": "MONTHLY",
                    "BudgetType": "COST",
                },
            )
            resp = budgets.describe_budget(AccountId=ACCOUNT_ID, BudgetName=name)
            assert resp["Budget"]["BudgetLimit"]["Amount"] == "200"
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)


class TestBudgetActionOperations:
    """Tests for DescribeBudgetActionsForAccount and DescribeBudgetActionsForBudget."""

    def test_describe_budget_actions_for_account(self, budgets):
        resp = budgets.describe_budget_actions_for_account(AccountId=ACCOUNT_ID)
        assert "Actions" in resp
        assert isinstance(resp["Actions"], list)

    def test_describe_budget_actions_for_budget(self, budgets):
        name = _unique("budget")
        budgets.create_budget(
            AccountId=ACCOUNT_ID,
            Budget={
                "BudgetName": name,
                "BudgetLimit": {"Amount": "100", "Unit": "USD"},
                "TimeUnit": "MONTHLY",
                "BudgetType": "COST",
            },
        )
        try:
            resp = budgets.describe_budget_actions_for_budget(AccountId=ACCOUNT_ID, BudgetName=name)
            assert "Actions" in resp
            assert isinstance(resp["Actions"], list)
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)


class TestBudgetNotificationsForAccount:
    """Tests for DescribeBudgetNotificationsForAccount."""

    def test_describe_budget_notifications_for_account(self, budgets):
        resp = budgets.describe_budget_notifications_for_account(AccountId=ACCOUNT_ID)
        assert "BudgetNotificationsForAccount" in resp
        assert isinstance(resp["BudgetNotificationsForAccount"], list)


class TestBudgetPerformanceHistory:
    """Tests for DescribeBudgetPerformanceHistory."""

    def test_describe_budget_performance_history(self, budgets):
        name = _unique("budget")
        budgets.create_budget(
            AccountId=ACCOUNT_ID,
            Budget={
                "BudgetName": name,
                "BudgetLimit": {"Amount": "100", "Unit": "USD"},
                "TimeUnit": "MONTHLY",
                "BudgetType": "COST",
            },
        )
        try:
            resp = budgets.describe_budget_performance_history(
                AccountId=ACCOUNT_ID, BudgetName=name
            )
            assert "BudgetPerformanceHistory" in resp
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)


class TestBudgetTagOperations:
    """Tests for ListTagsForResource, TagResource, UntagResource."""

    def _create_budget_arn(self, budgets, name):
        budgets.create_budget(
            AccountId=ACCOUNT_ID,
            Budget={
                "BudgetName": name,
                "BudgetLimit": {"Amount": "100", "Unit": "USD"},
                "TimeUnit": "MONTHLY",
                "BudgetType": "COST",
            },
        )
        return f"arn:aws:budgets::{ACCOUNT_ID}:budget/{name}"

    def test_list_tags_for_resource(self, budgets):
        name = _unique("budget")
        arn = self._create_budget_arn(budgets, name)
        try:
            resp = budgets.list_tags_for_resource(ResourceARN=arn)
            assert "ResourceTags" in resp
            assert isinstance(resp["ResourceTags"], list)
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)

    def test_tag_resource(self, budgets):
        name = _unique("budget")
        arn = self._create_budget_arn(budgets, name)
        try:
            budgets.tag_resource(
                ResourceARN=arn,
                ResourceTags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "project", "Value": "roboto"},
                ],
            )
            resp = budgets.list_tags_for_resource(ResourceARN=arn)
            tags = {t["Key"]: t["Value"] for t in resp["ResourceTags"]}
            assert tags["env"] == "test"
            assert tags["project"] == "roboto"
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)

    def test_untag_resource(self, budgets):
        name = _unique("budget")
        arn = self._create_budget_arn(budgets, name)
        try:
            budgets.tag_resource(
                ResourceARN=arn,
                ResourceTags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "keep", "Value": "yes"},
                ],
            )
            budgets.untag_resource(ResourceARN=arn, ResourceTagKeys=["env"])
            resp = budgets.list_tags_for_resource(ResourceARN=arn)
            tags = {t["Key"]: t["Value"] for t in resp["ResourceTags"]}
            assert "env" not in tags
            assert tags["keep"] == "yes"
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)
