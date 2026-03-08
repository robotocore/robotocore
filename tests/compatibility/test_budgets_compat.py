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
