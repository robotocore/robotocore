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

    def test_list_user_settings(self, client):
        """ListUserSettings returns a response."""
        resp = client.list_user_settings()
        assert "userSettings" in resp
