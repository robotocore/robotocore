"""WorkSpaces Web compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def workspacesweb():
    return make_client("workspaces-web")


class TestWorkSpacesWebPortals:
    def test_list_portals(self, workspacesweb):
        resp = workspacesweb.list_portals()
        assert "portals" in resp
        assert isinstance(resp["portals"], list)

    def test_create_portal(self, workspacesweb):
        resp = workspacesweb.create_portal()
        assert "portalArn" in resp
        assert resp["portalArn"].startswith("arn:aws:workspaces-web:")
        assert "portalEndpoint" in resp

    def test_get_portal(self, workspacesweb):
        create_resp = workspacesweb.create_portal()
        arn = create_resp["portalArn"]

        get_resp = workspacesweb.get_portal(portalArn=arn)
        portal = get_resp["portal"]
        assert portal["portalArn"] == arn
        assert "portalStatus" in portal
        assert "portalEndpoint" in portal

    def test_delete_portal(self, workspacesweb):
        create_resp = workspacesweb.create_portal()
        arn = create_resp["portalArn"]

        delete_resp = workspacesweb.delete_portal(portalArn=arn)
        assert delete_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_and_list_portals(self, workspacesweb):
        resp1 = workspacesweb.create_portal()
        arn = resp1["portalArn"]

        listed = workspacesweb.list_portals()["portals"]
        listed_arns = [p["portalArn"] for p in listed]
        assert arn in listed_arns


class TestWorkSpacesWebBrowserSettings:
    def test_list_browser_settings(self, workspacesweb):
        resp = workspacesweb.list_browser_settings()
        assert "browserSettings" in resp
        assert isinstance(resp["browserSettings"], list)

    def test_create_browser_settings(self, workspacesweb):
        resp = workspacesweb.create_browser_settings(browserPolicy="{}")
        assert "browserSettingsArn" in resp
        assert resp["browserSettingsArn"].startswith("arn:aws:workspaces-web:")

    def test_create_and_list_browser_settings(self, workspacesweb):
        resp = workspacesweb.create_browser_settings(browserPolicy="{}")
        arn = resp["browserSettingsArn"]

        listed = workspacesweb.list_browser_settings()["browserSettings"]
        listed_arns = [bs["browserSettingsArn"] for bs in listed]
        assert arn in listed_arns


class TestWorkSpacesWebNetworkSettings:
    def test_list_network_settings(self, workspacesweb):
        resp = workspacesweb.list_network_settings()
        assert "networkSettings" in resp
        assert isinstance(resp["networkSettings"], list)

    def test_create_network_settings(self, workspacesweb):
        resp = workspacesweb.create_network_settings(
            vpcId="vpc-" + uuid.uuid4().hex[:8],
            subnetIds=["subnet-" + uuid.uuid4().hex[:8], "subnet-" + uuid.uuid4().hex[:8]],
            securityGroupIds=["sg-" + uuid.uuid4().hex[:8]],
        )
        assert "networkSettingsArn" in resp
        assert resp["networkSettingsArn"].startswith("arn:aws:workspaces-web:")

    def test_create_and_list_network_settings(self, workspacesweb):
        resp = workspacesweb.create_network_settings(
            vpcId="vpc-" + uuid.uuid4().hex[:8],
            subnetIds=["subnet-" + uuid.uuid4().hex[:8], "subnet-" + uuid.uuid4().hex[:8]],
            securityGroupIds=["sg-" + uuid.uuid4().hex[:8]],
        )
        arn = resp["networkSettingsArn"]

        listed = workspacesweb.list_network_settings()["networkSettings"]
        listed_arns = [ns["networkSettingsArn"] for ns in listed]
        assert arn in listed_arns


class TestWorkspaceswebAutoCoverage:
    """Auto-generated coverage tests for workspacesweb."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces-web")

    def test_list_user_access_logging_settings(self, client):
        """ListUserAccessLoggingSettings returns a response."""
        resp = client.list_user_access_logging_settings()
        assert "userAccessLoggingSettings" in resp
        assert isinstance(resp["userAccessLoggingSettings"], list)

    def test_list_user_settings(self, client):
        """ListUserSettings returns a response."""
        resp = client.list_user_settings()
        assert "userSettings" in resp
        assert isinstance(resp["userSettings"], list)


class TestWorkspaceswebUserAccessLoggingSettings:
    """Tests for UserAccessLoggingSettings CRUD."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces-web")

    def test_create_user_access_logging_settings(self, client):
        """CreateUserAccessLoggingSettings returns an ARN."""
        resp = client.create_user_access_logging_settings(
            kinesisStreamArn="arn:aws:kinesis:us-east-1:123456789012:stream/test-stream"
        )
        assert "userAccessLoggingSettingsArn" in resp
        assert resp["userAccessLoggingSettingsArn"].startswith("arn:aws:workspaces-web:")

    def test_get_user_access_logging_settings(self, client):
        """GetUserAccessLoggingSettings returns the settings."""
        create_resp = client.create_user_access_logging_settings(
            kinesisStreamArn="arn:aws:kinesis:us-east-1:123456789012:stream/test-stream"
        )
        arn = create_resp["userAccessLoggingSettingsArn"]
        resp = client.get_user_access_logging_settings(userAccessLoggingSettingsArn=arn)
        assert "userAccessLoggingSettings" in resp
        assert resp["userAccessLoggingSettings"]["userAccessLoggingSettingsArn"] == arn

    def test_delete_user_access_logging_settings(self, client):
        """DeleteUserAccessLoggingSettings removes the settings."""
        create_resp = client.create_user_access_logging_settings(
            kinesisStreamArn="arn:aws:kinesis:us-east-1:123456789012:stream/test-stream"
        )
        arn = create_resp["userAccessLoggingSettingsArn"]
        resp = client.delete_user_access_logging_settings(userAccessLoggingSettingsArn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkspaceswebUserSettings:
    """Tests for UserSettings CRUD."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces-web")

    def test_create_user_settings(self, client):
        """CreateUserSettings returns an ARN."""
        resp = client.create_user_settings(
            copyAllowed="Enabled",
            pasteAllowed="Enabled",
            downloadAllowed="Enabled",
            uploadAllowed="Enabled",
            printAllowed="Enabled",
        )
        assert "userSettingsArn" in resp
        assert resp["userSettingsArn"].startswith("arn:aws:workspaces-web:")

    def test_get_user_settings(self, client):
        """GetUserSettings returns the settings."""
        create_resp = client.create_user_settings(
            copyAllowed="Enabled",
            pasteAllowed="Enabled",
            downloadAllowed="Enabled",
            uploadAllowed="Enabled",
            printAllowed="Enabled",
        )
        arn = create_resp["userSettingsArn"]
        resp = client.get_user_settings(userSettingsArn=arn)
        assert "userSettings" in resp
        assert resp["userSettings"]["userSettingsArn"] == arn
        assert resp["userSettings"]["copyAllowed"] == "Enabled"

    def test_delete_user_settings(self, client):
        """DeleteUserSettings removes the settings."""
        create_resp = client.create_user_settings(
            copyAllowed="Enabled",
            pasteAllowed="Enabled",
            downloadAllowed="Enabled",
            uploadAllowed="Enabled",
            printAllowed="Enabled",
        )
        arn = create_resp["userSettingsArn"]
        resp = client.delete_user_settings(userSettingsArn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkspaceswebBrowserSettingsCRUD:
    """Tests for BrowserSettings Get/Delete."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces-web")

    def test_get_browser_settings(self, client):
        """GetBrowserSettings returns the settings."""
        create_resp = client.create_browser_settings(
            browserPolicy='{"version":"2012-10-17","statement":[]}'
        )
        arn = create_resp["browserSettingsArn"]
        resp = client.get_browser_settings(browserSettingsArn=arn)
        assert "browserSettings" in resp
        assert resp["browserSettings"]["browserSettingsArn"] == arn

    def test_delete_browser_settings(self, client):
        """DeleteBrowserSettings removes the settings."""
        create_resp = client.create_browser_settings(
            browserPolicy='{"version":"2012-10-17","statement":[]}'
        )
        arn = create_resp["browserSettingsArn"]
        resp = client.delete_browser_settings(browserSettingsArn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkspaceswebNetworkSettingsCRUD:
    """Tests for NetworkSettings Get/Delete."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces-web")

    def test_get_network_settings(self, client):
        """GetNetworkSettings returns the settings."""
        create_resp = client.create_network_settings(
            vpcId="vpc-12345678",
            subnetIds=["subnet-12345678", "subnet-87654321"],
            securityGroupIds=["sg-12345678"],
        )
        arn = create_resp["networkSettingsArn"]
        resp = client.get_network_settings(networkSettingsArn=arn)
        assert "networkSettings" in resp
        assert resp["networkSettings"]["networkSettingsArn"] == arn
        assert resp["networkSettings"]["vpcId"] == "vpc-12345678"

    def test_delete_network_settings(self, client):
        """DeleteNetworkSettings removes the settings."""
        create_resp = client.create_network_settings(
            vpcId="vpc-12345678",
            subnetIds=["subnet-12345678", "subnet-87654321"],
            securityGroupIds=["sg-12345678"],
        )
        arn = create_resp["networkSettingsArn"]
        resp = client.delete_network_settings(networkSettingsArn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkspaceswebAssociations:
    """Tests for Associate* operations."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces-web")

    @pytest.fixture
    def portal_arn(self, client):
        resp = client.create_portal(displayName="Test Portal")
        return resp["portalArn"]

    def test_associate_browser_settings(self, client, portal_arn):
        """AssociateBrowserSettings links settings to a portal."""
        bs = client.create_browser_settings(browserPolicy='{"version":"2012-10-17","statement":[]}')
        bs_arn = bs["browserSettingsArn"]
        resp = client.associate_browser_settings(
            portalArn=portal_arn,
            browserSettingsArn=bs_arn,
        )
        assert resp["portalArn"] == portal_arn
        assert resp["browserSettingsArn"] == bs_arn

    def test_associate_network_settings(self, client, portal_arn):
        """AssociateNetworkSettings links settings to a portal."""
        ns = client.create_network_settings(
            vpcId="vpc-12345678",
            subnetIds=["subnet-12345678", "subnet-87654321"],
            securityGroupIds=["sg-12345678"],
        )
        ns_arn = ns["networkSettingsArn"]
        resp = client.associate_network_settings(
            portalArn=portal_arn,
            networkSettingsArn=ns_arn,
        )
        assert resp["portalArn"] == portal_arn
        assert resp["networkSettingsArn"] == ns_arn

    def test_associate_user_access_logging_settings(self, client, portal_arn):
        """AssociateUserAccessLoggingSettings links settings to a portal."""
        uals = client.create_user_access_logging_settings(
            kinesisStreamArn="arn:aws:kinesis:us-east-1:123456789012:stream/test-stream"
        )
        uals_arn = uals["userAccessLoggingSettingsArn"]
        resp = client.associate_user_access_logging_settings(
            portalArn=portal_arn,
            userAccessLoggingSettingsArn=uals_arn,
        )
        # Response contains both ARNs; verify the correct ARNs appear in the response
        all_arns = {resp.get("portalArn"), resp.get("userAccessLoggingSettingsArn")}
        assert portal_arn in all_arns
        assert uals_arn in all_arns

    def test_associate_user_settings(self, client, portal_arn):
        """AssociateUserSettings links settings to a portal."""
        us = client.create_user_settings(
            copyAllowed="Enabled",
            pasteAllowed="Enabled",
            downloadAllowed="Enabled",
            uploadAllowed="Enabled",
            printAllowed="Enabled",
        )
        us_arn = us["userSettingsArn"]
        resp = client.associate_user_settings(
            portalArn=portal_arn,
            userSettingsArn=us_arn,
        )
        # Response contains both ARNs; verify the correct ARNs appear in the response
        all_arns = {resp.get("portalArn"), resp.get("userSettingsArn")}
        assert portal_arn in all_arns
        assert us_arn in all_arns


class TestWorkspaceswebTagging:
    """Tests for TagResource, ListTagsForResource, UntagResource."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces-web")

    @pytest.fixture
    def resource_arn(self, client):
        resp = client.create_browser_settings(
            browserPolicy='{"version":"2012-10-17","statement":[]}'
        )
        return resp["browserSettingsArn"]

    def test_tag_resource(self, client, resource_arn):
        """TagResource returns 200."""
        resp = client.tag_resource(
            resourceArn=resource_arn,
            tags=[{"Key": "Env", "Value": "test"}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_tags_for_resource(self, client, resource_arn):
        """ListTagsForResource returns the tags we set."""
        client.tag_resource(
            resourceArn=resource_arn,
            tags=[{"Key": "Env", "Value": "test"}, {"Key": "Team", "Value": "dev"}],
        )
        resp = client.list_tags_for_resource(resourceArn=resource_arn)
        assert "tags" in resp
        tag_map = {t["Key"]: t["Value"] for t in resp["tags"]}
        assert tag_map.get("Env") == "test"
        assert tag_map.get("Team") == "dev"

    def test_untag_resource(self, client, resource_arn):
        """UntagResource removes specific tags."""
        client.tag_resource(
            resourceArn=resource_arn,
            tags=[{"Key": "Env", "Value": "test"}, {"Key": "Team", "Value": "dev"}],
        )
        resp = client.untag_resource(resourceArn=resource_arn, tagKeys=["Env"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        tags_resp = client.list_tags_for_resource(resourceArn=resource_arn)
        keys = [t["Key"] for t in tags_resp["tags"]]
        assert "Env" not in keys
        assert "Team" in keys
