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


class TestBudgetActionLifecycle:
    """Tests for budget action CRUD operations."""

    def _make_budget(self, budgets, name):
        budgets.create_budget(
            AccountId=ACCOUNT_ID,
            Budget={
                "BudgetName": name,
                "BudgetLimit": {"Amount": "100", "Unit": "USD"},
                "TimeUnit": "MONTHLY",
                "BudgetType": "COST",
            },
        )

    def _create_action(self, budgets, budget_name):
        resp = budgets.create_budget_action(
            AccountId=ACCOUNT_ID,
            BudgetName=budget_name,
            NotificationType="ACTUAL",
            ActionType="APPLY_IAM_POLICY",
            ActionThreshold={
                "ActionThresholdValue": 80.0,
                "ActionThresholdType": "PERCENTAGE",
            },
            Definition={
                "IamActionDefinition": {
                    "PolicyArn": "arn:aws:iam::123456789012:policy/test",
                    "Roles": ["test-role"],
                }
            },
            ExecutionRoleArn="arn:aws:iam::123456789012:role/test-role",
            ApprovalModel="AUTOMATIC",
            Subscribers=[
                {"SubscriptionType": "EMAIL", "Address": "test@example.com"},
            ],
        )
        return resp["ActionId"]

    def test_create_budget_action(self, budgets):
        name = _unique("budget")
        self._make_budget(budgets, name)
        try:
            action_id = self._create_action(budgets, name)
            assert len(action_id) == 36  # UUID format
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)

    def test_describe_budget_action(self, budgets):
        name = _unique("budget")
        self._make_budget(budgets, name)
        try:
            action_id = self._create_action(budgets, name)
            resp = budgets.describe_budget_action(
                AccountId=ACCOUNT_ID, BudgetName=name, ActionId=action_id
            )
            action = resp["Action"]
            assert action["ActionId"] == action_id
            assert action["BudgetName"] == name
            assert action["NotificationType"] == "ACTUAL"
            assert action["ActionType"] == "APPLY_IAM_POLICY"
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)

    def test_update_budget_action(self, budgets):
        name = _unique("budget")
        self._make_budget(budgets, name)
        try:
            action_id = self._create_action(budgets, name)
            resp = budgets.update_budget_action(
                AccountId=ACCOUNT_ID,
                BudgetName=name,
                ActionId=action_id,
                NotificationType="FORECASTED",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            new_action = resp["NewAction"]
            assert new_action["NotificationType"] == "FORECASTED"
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)

    def test_execute_budget_action(self, budgets):
        name = _unique("budget")
        self._make_budget(budgets, name)
        try:
            action_id = self._create_action(budgets, name)
            resp = budgets.execute_budget_action(
                AccountId=ACCOUNT_ID,
                BudgetName=name,
                ActionId=action_id,
                ExecutionType="APPROVE_BUDGET_ACTION",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert resp["AccountId"] == ACCOUNT_ID
            assert resp["BudgetName"] == name
            assert resp["ActionId"] == action_id
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)

    def test_delete_budget_action(self, budgets):
        name = _unique("budget")
        self._make_budget(budgets, name)
        try:
            action_id = self._create_action(budgets, name)
            resp = budgets.delete_budget_action(
                AccountId=ACCOUNT_ID, BudgetName=name, ActionId=action_id
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify it's gone
            actions_resp = budgets.describe_budget_actions_for_budget(
                AccountId=ACCOUNT_ID, BudgetName=name
            )
            action_ids = [a["ActionId"] for a in actions_resp["Actions"]]
            assert action_id not in action_ids
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)

    def test_actions_appear_in_account_list(self, budgets):
        name = _unique("budget")
        self._make_budget(budgets, name)
        try:
            action_id = self._create_action(budgets, name)
            resp = budgets.describe_budget_actions_for_account(AccountId=ACCOUNT_ID)
            action_ids = [a["ActionId"] for a in resp["Actions"]]
            assert action_id in action_ids
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)


class TestBudgetSubscriberOperations:
    """Tests for subscriber CRUD operations."""

    def test_create_subscriber(self, budgets):
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
        notification = {
            "NotificationType": "ACTUAL",
            "ComparisonOperator": "GREATER_THAN",
            "Threshold": 80.0,
            "ThresholdType": "PERCENTAGE",
        }
        try:
            budgets.create_notification(
                AccountId=ACCOUNT_ID,
                BudgetName=name,
                Notification=notification,
                Subscribers=[
                    {"SubscriptionType": "EMAIL", "Address": "orig@example.com"},
                ],
            )
            resp = budgets.create_subscriber(
                AccountId=ACCOUNT_ID,
                BudgetName=name,
                Notification=notification,
                Subscriber={"SubscriptionType": "EMAIL", "Address": "new@example.com"},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)

    def test_delete_subscriber(self, budgets):
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
        notification = {
            "NotificationType": "ACTUAL",
            "ComparisonOperator": "GREATER_THAN",
            "Threshold": 80.0,
            "ThresholdType": "PERCENTAGE",
        }
        try:
            budgets.create_notification(
                AccountId=ACCOUNT_ID,
                BudgetName=name,
                Notification=notification,
                Subscribers=[
                    {"SubscriptionType": "EMAIL", "Address": "orig@example.com"},
                    {"SubscriptionType": "EMAIL", "Address": "extra@example.com"},
                ],
            )
            resp = budgets.delete_subscriber(
                AccountId=ACCOUNT_ID,
                BudgetName=name,
                Notification=notification,
                Subscriber={"SubscriptionType": "EMAIL", "Address": "extra@example.com"},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)


class TestBudgetNotificationUpdateOperations:
    """Tests for UpdateNotification and UpdateSubscriber."""

    def test_update_notification(self, budgets):
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
        old_notification = {
            "NotificationType": "ACTUAL",
            "ComparisonOperator": "GREATER_THAN",
            "Threshold": 80.0,
            "ThresholdType": "PERCENTAGE",
        }
        try:
            budgets.create_notification(
                AccountId=ACCOUNT_ID,
                BudgetName=name,
                Notification=old_notification,
                Subscribers=[
                    {"SubscriptionType": "EMAIL", "Address": "test@example.com"},
                ],
            )
            new_notification = old_notification.copy()
            new_notification["Threshold"] = 95.0
            resp = budgets.update_notification(
                AccountId=ACCOUNT_ID,
                BudgetName=name,
                OldNotification=old_notification,
                NewNotification=new_notification,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify the update took effect
            desc = budgets.describe_notifications_for_budget(AccountId=ACCOUNT_ID, BudgetName=name)
            thresholds = [n["Threshold"] for n in desc["Notifications"]]
            assert 95.0 in thresholds
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)

    def test_update_subscriber(self, budgets):
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
        notification = {
            "NotificationType": "ACTUAL",
            "ComparisonOperator": "GREATER_THAN",
            "Threshold": 80.0,
            "ThresholdType": "PERCENTAGE",
        }
        try:
            budgets.create_notification(
                AccountId=ACCOUNT_ID,
                BudgetName=name,
                Notification=notification,
                Subscribers=[
                    {"SubscriptionType": "EMAIL", "Address": "old@example.com"},
                ],
            )
            resp = budgets.update_subscriber(
                AccountId=ACCOUNT_ID,
                BudgetName=name,
                Notification=notification,
                OldSubscriber={"SubscriptionType": "EMAIL", "Address": "old@example.com"},
                NewSubscriber={"SubscriptionType": "EMAIL", "Address": "new@example.com"},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
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


class TestBudgetActionHistories:
    """Tests for DescribeBudgetActionHistories."""

    def _make_budget_with_action(self, budgets):
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
        resp = budgets.create_budget_action(
            AccountId=ACCOUNT_ID,
            BudgetName=name,
            NotificationType="ACTUAL",
            ActionType="APPLY_IAM_POLICY",
            ActionThreshold={
                "ActionThresholdValue": 80.0,
                "ActionThresholdType": "PERCENTAGE",
            },
            Definition={
                "IamActionDefinition": {
                    "PolicyArn": "arn:aws:iam::123456789012:policy/test",
                    "Roles": ["test-role"],
                }
            },
            ExecutionRoleArn="arn:aws:iam::123456789012:role/test-role",
            ApprovalModel="AUTOMATIC",
            Subscribers=[
                {"SubscriptionType": "EMAIL", "Address": "test@example.com"},
            ],
        )
        return name, resp["ActionId"]

    def test_describe_budget_action_histories(self, budgets):
        """DescribeBudgetActionHistories returns ActionHistories list."""
        name, action_id = self._make_budget_with_action(budgets)
        try:
            resp = budgets.describe_budget_action_histories(
                AccountId=ACCOUNT_ID, BudgetName=name, ActionId=action_id
            )
            assert "ActionHistories" in resp
            assert isinstance(resp["ActionHistories"], list)
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)


class TestBudgetSubscribersForNotification:
    """Tests for DescribeSubscribersForNotification."""

    def test_describe_subscribers_for_notification(self, budgets):
        """DescribeSubscribersForNotification returns subscribers list."""
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
        notification = {
            "NotificationType": "ACTUAL",
            "ComparisonOperator": "GREATER_THAN",
            "Threshold": 80.0,
            "ThresholdType": "PERCENTAGE",
        }
        try:
            budgets.create_notification(
                AccountId=ACCOUNT_ID,
                BudgetName=name,
                Notification=notification,
                Subscribers=[
                    {"SubscriptionType": "EMAIL", "Address": "sub1@example.com"},
                    {"SubscriptionType": "EMAIL", "Address": "sub2@example.com"},
                ],
            )
            resp = budgets.describe_subscribers_for_notification(
                AccountId=ACCOUNT_ID, BudgetName=name, Notification=notification
            )
            assert "Subscribers" in resp
            assert len(resp["Subscribers"]) == 2
            addresses = {s["Address"] for s in resp["Subscribers"]}
            assert "sub1@example.com" in addresses
            assert "sub2@example.com" in addresses
        finally:
            budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)
