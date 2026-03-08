"""Inspector2 compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def inspector2():
    return make_client("inspector2")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestInspector2FindingsOperations:
    def test_list_findings(self, inspector2):
        resp = inspector2.list_findings()
        assert "findings" in resp
        assert isinstance(resp["findings"], list)

    def test_batch_get_account_status(self, inspector2):
        resp = inspector2.batch_get_account_status(accountIds=["123456789012"])
        assert "accounts" in resp
        assert "failedAccounts" in resp
        assert isinstance(resp["accounts"], list)


class TestInspector2FilterOperations:
    def test_list_filters(self, inspector2):
        resp = inspector2.list_filters()
        assert "filters" in resp
        assert isinstance(resp["filters"], list)

    def test_create_filter(self, inspector2):
        name = _unique("filter")
        resp = inspector2.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        assert "arn" in resp
        assert resp["arn"]


class TestInspector2OrganizationOperations:
    def test_list_members(self, inspector2):
        resp = inspector2.list_members()
        assert "members" in resp
        assert isinstance(resp["members"], list)

    def test_describe_organization_configuration(self, inspector2):
        resp = inspector2.describe_organization_configuration()
        assert "autoEnable" in resp
        auto_enable = resp["autoEnable"]
        assert "ec2" in auto_enable
        assert "ecr" in auto_enable
        assert "lambda" in auto_enable


class TestInspector2AutoCoverage:
    """Auto-generated coverage tests for inspector2."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_list_delegated_admin_accounts(self, client):
        """ListDelegatedAdminAccounts returns a response."""
        resp = client.list_delegated_admin_accounts()
        assert "delegatedAdminAccounts" in resp


class TestInspector2MemberOperations:
    """Tests for Member operations: AssociateMember, GetMember, DisassociateMember."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_associate_member(self, client):
        """AssociateMember associates a member account."""
        resp = client.associate_member(accountId="210987654321")
        assert "accountId" in resp
        assert resp["accountId"] == "210987654321"

    def test_get_member(self, client):
        """GetMember retrieves a member account after association."""
        client.associate_member(accountId="210987654321")
        resp = client.get_member(accountId="210987654321")
        assert "member" in resp
        member = resp["member"]
        assert "accountId" in member

    def test_disassociate_member(self, client):
        """DisassociateMember removes a member account."""
        client.associate_member(accountId="210987654321")
        resp = client.disassociate_member(accountId="210987654321")
        assert "accountId" in resp
        assert resp["accountId"] == "210987654321"


class TestInspector2DelegatedAdminOperations:
    """Tests for EnableDelegatedAdminAccount, DisableDelegatedAdminAccount."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_enable_delegated_admin_account(self, client):
        """EnableDelegatedAdminAccount enables a delegated admin."""
        resp = client.enable_delegated_admin_account(delegatedAdminAccountId="210987654321")
        assert "delegatedAdminAccountId" in resp
        assert resp["delegatedAdminAccountId"] == "210987654321"

    def test_disable_delegated_admin_account(self, client):
        """DisableDelegatedAdminAccount disables a delegated admin."""
        client.enable_delegated_admin_account(delegatedAdminAccountId="210987654321")
        resp = client.disable_delegated_admin_account(delegatedAdminAccountId="210987654321")
        assert "delegatedAdminAccountId" in resp
        assert resp["delegatedAdminAccountId"] == "210987654321"


class TestInspector2DeleteFilterOperation:
    """Test for DeleteFilter."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_delete_filter(self, client):
        """DeleteFilter removes a previously created filter."""
        name = _unique("filter")
        create_resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = create_resp["arn"]
        resp = client.delete_filter(arn=arn)
        assert "arn" in resp
        assert resp["arn"] == arn


class TestInspector2UpdateOrgConfigOperation:
    """Test for UpdateOrganizationConfiguration."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_update_organization_configuration(self, client):
        """UpdateOrganizationConfiguration updates auto-enable settings."""
        resp = client.update_organization_configuration(
            autoEnable={"ec2": True, "ecr": True, "lambda": False}
        )
        assert "autoEnable" in resp
        auto_enable = resp["autoEnable"]
        assert "ec2" in auto_enable
        assert "ecr" in auto_enable


class TestInspector2TagOperations:
    """Tests for TagResource, UntagResource, and ListTagsForResource."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_tag_resource_and_list_tags(self, client):
        """TagResource adds tags, ListTagsForResource reads them."""
        name = _unique("filter")
        create_resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = create_resp["arn"]
        client.tag_resource(resourceArn=arn, tags={"env": "test"})
        resp = client.list_tags_for_resource(resourceArn=arn)
        assert "tags" in resp

    def test_list_tags_for_resource(self, client):
        """ListTagsForResource on a filter with no extra tags."""
        name = _unique("filter")
        create_resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = create_resp["arn"]
        resp = client.list_tags_for_resource(resourceArn=arn)
        assert "tags" in resp

    def test_untag_resource(self, client):
        """UntagResource removes a tag from a filter."""
        name = _unique("filter")
        create_resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = create_resp["arn"]
        client.tag_resource(resourceArn=arn, tags={"env": "test", "team": "backend"})
        resp = client.untag_resource(resourceArn=arn, tagKeys=["env"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestInspector2EnableDisableOperations:
    """Tests for Enable and Disable operations."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_enable_ec2_scanning(self, client):
        """Enable EC2 scanning for an account."""
        resp = client.enable(resourceTypes=["EC2"], accountIds=["123456789012"])
        assert "accounts" in resp
        assert "failedAccounts" in resp
        assert isinstance(resp["accounts"], list)
        if resp["accounts"]:
            acct = resp["accounts"][0]
            assert acct["accountId"] == "123456789012"
            assert acct["status"] == "ENABLED"
            assert "resourceStatus" in acct
            assert acct["resourceStatus"]["ec2"] == "ENABLED"

    def test_disable_ec2_scanning(self, client):
        """Disable EC2 scanning for an account."""
        # Disable all resource types first to get a clean state
        try:
            client.disable(
                resourceTypes=["EC2", "ECR", "LAMBDA"],
                accountIds=["123456789012"],
            )
        except Exception:
            pass
        client.enable(resourceTypes=["EC2"], accountIds=["123456789012"])
        resp = client.disable(resourceTypes=["EC2"], accountIds=["123456789012"])
        assert "accounts" in resp
        assert isinstance(resp["accounts"], list)
        if resp["accounts"]:
            acct = resp["accounts"][0]
            assert acct["accountId"] == "123456789012"
            assert acct["resourceStatus"]["ec2"] == "DISABLED"

    def test_enable_multiple_resource_types(self, client):
        """Enable scanning for multiple resource types."""
        resp = client.enable(
            resourceTypes=["EC2", "ECR"],
            accountIds=["123456789012"],
        )
        assert "accounts" in resp
        assert "failedAccounts" in resp
        if resp["accounts"]:
            status = resp["accounts"][0]["resourceStatus"]
            assert status["ec2"] == "ENABLED"
            assert status["ecr"] == "ENABLED"

    def test_enable_returns_empty_failed_accounts(self, client):
        """Enable returns empty failedAccounts when operation succeeds."""
        resp = client.enable(resourceTypes=["EC2"], accountIds=["123456789012"])
        assert resp["failedAccounts"] == []

    def test_disable_returns_empty_failed_accounts(self, client):
        """Disable returns empty failedAccounts when operation succeeds."""
        client.enable(resourceTypes=["EC2"], accountIds=["123456789012"])
        resp = client.disable(resourceTypes=["EC2"], accountIds=["123456789012"])
        assert resp["failedAccounts"] == []


class TestInspector2FilterAdvanced:
    """Advanced filter operation tests."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_create_filter_returns_arn(self, client):
        """CreateFilter returns a valid-looking ARN."""
        name = _unique("filter")
        resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = resp["arn"]
        assert "arn:" in arn
        assert "inspector2" in arn.lower() or "filter" in arn.lower()

    def test_list_filters_includes_created(self, client):
        """ListFilters includes a newly created filter."""
        name = _unique("filter")
        create_resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = create_resp["arn"]
        resp = client.list_filters()
        arns = [f["arn"] for f in resp["filters"]]
        assert arn in arns

    def test_create_filter_with_suppress_action(self, client):
        """CreateFilter with SUPPRESS action."""
        name = _unique("filter")
        resp = client.create_filter(
            action="SUPPRESS",
            filterCriteria={},
            name=name,
        )
        assert "arn" in resp
        assert resp["arn"]

    def test_delete_filter_removes_from_list(self, client):
        """DeleteFilter removes the filter from ListFilters results."""
        name = _unique("filter")
        create_resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = create_resp["arn"]
        client.delete_filter(arn=arn)
        resp = client.list_filters()
        arns = [f["arn"] for f in resp["filters"]]
        assert arn not in arns
