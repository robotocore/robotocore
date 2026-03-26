"""WorkSpaces Web compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

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


class TestWorkspaceswebPortalUpdateAndMetadata:
    """Tests for UpdatePortal and GetPortalServiceProviderMetadata."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces-web")

    def test_update_portal(self, client):
        """UpdatePortal returns updated portal data."""
        create_resp = client.create_portal(displayName="Original")
        arn = create_resp["portalArn"]
        resp = client.update_portal(portalArn=arn, displayName="Updated")
        assert "portal" in resp
        assert resp["portal"]["portalArn"] == arn
        assert resp["portal"]["displayName"] == "Updated"

    def test_get_portal_service_provider_metadata(self, client):
        """GetPortalServiceProviderMetadata returns SAML metadata."""
        create_resp = client.create_portal()
        arn = create_resp["portalArn"]
        resp = client.get_portal_service_provider_metadata(portalArn=arn)
        assert "portalArn" in resp
        assert resp["portalArn"] == arn
        assert "serviceProviderSamlMetadata" in resp


class TestWorkspaceswebUpdateOperations:
    """Tests for Update* operations on existing resources."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces-web")

    def test_update_browser_settings(self, client):
        """UpdateBrowserSettings returns updated browser settings."""
        create_resp = client.create_browser_settings(browserPolicy='{"rules":[]}')
        arn = create_resp["browserSettingsArn"]
        resp = client.update_browser_settings(
            browserSettingsArn=arn, browserPolicy='{"rules":[{}]}'
        )
        assert "browserSettings" in resp
        assert resp["browserSettings"]["browserSettingsArn"] == arn

    def test_update_network_settings(self, client):
        """UpdateNetworkSettings returns updated network settings."""
        create_resp = client.create_network_settings(
            vpcId="vpc-11111111",
            subnetIds=["subnet-11111111", "subnet-22222222"],
            securityGroupIds=["sg-11111111"],
        )
        arn = create_resp["networkSettingsArn"]
        resp = client.update_network_settings(networkSettingsArn=arn, vpcId="vpc-22222222")
        assert "networkSettings" in resp
        assert resp["networkSettings"]["vpcId"] == "vpc-22222222"

    def test_update_user_settings(self, client):
        """UpdateUserSettings returns updated user settings."""
        create_resp = client.create_user_settings(
            copyAllowed="Enabled",
            pasteAllowed="Enabled",
            downloadAllowed="Enabled",
            uploadAllowed="Enabled",
            printAllowed="Enabled",
        )
        arn = create_resp["userSettingsArn"]
        resp = client.update_user_settings(userSettingsArn=arn, copyAllowed="Disabled")
        assert "userSettings" in resp
        assert resp["userSettings"]["copyAllowed"] == "Disabled"

    def test_update_user_access_logging_settings(self, client):
        """UpdateUserAccessLoggingSettings returns updated settings."""
        create_resp = client.create_user_access_logging_settings(
            kinesisStreamArn="arn:aws:kinesis:us-east-1:123456789012:stream/original"
        )
        arn = create_resp["userAccessLoggingSettingsArn"]
        resp = client.update_user_access_logging_settings(
            userAccessLoggingSettingsArn=arn,
            kinesisStreamArn="arn:aws:kinesis:us-east-1:123456789012:stream/updated",
        )
        assert "userAccessLoggingSettings" in resp
        assert (
            resp["userAccessLoggingSettings"]["kinesisStreamArn"]
            == "arn:aws:kinesis:us-east-1:123456789012:stream/updated"
        )


class TestWorkspaceswebDisassociations:
    """Tests for Disassociate* operations."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces-web")

    @pytest.fixture
    def portal_arn(self, client):
        resp = client.create_portal(displayName="Test Portal")
        return resp["portalArn"]

    def test_disassociate_browser_settings(self, client, portal_arn):
        """DisassociateBrowserSettings removes browser settings from portal."""
        bs = client.create_browser_settings(browserPolicy='{"rules":[]}')
        client.associate_browser_settings(
            portalArn=portal_arn, browserSettingsArn=bs["browserSettingsArn"]
        )
        resp = client.disassociate_browser_settings(portalArn=portal_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disassociate_network_settings(self, client, portal_arn):
        """DisassociateNetworkSettings removes network settings from portal."""
        ns = client.create_network_settings(
            vpcId="vpc-12345678",
            subnetIds=["subnet-12345678", "subnet-87654321"],
            securityGroupIds=["sg-12345678"],
        )
        client.associate_network_settings(
            portalArn=portal_arn, networkSettingsArn=ns["networkSettingsArn"]
        )
        resp = client.disassociate_network_settings(portalArn=portal_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disassociate_user_settings(self, client, portal_arn):
        """DisassociateUserSettings removes user settings from portal."""
        us = client.create_user_settings(
            copyAllowed="Enabled",
            pasteAllowed="Enabled",
            downloadAllowed="Enabled",
            uploadAllowed="Enabled",
            printAllowed="Enabled",
        )
        client.associate_user_settings(portalArn=portal_arn, userSettingsArn=us["userSettingsArn"])
        resp = client.disassociate_user_settings(portalArn=portal_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disassociate_user_access_logging_settings(self, client, portal_arn):
        """DisassociateUserAccessLoggingSettings removes settings from portal."""
        uals = client.create_user_access_logging_settings(
            kinesisStreamArn="arn:aws:kinesis:us-east-1:123456789012:stream/test"
        )
        client.associate_user_access_logging_settings(
            portalArn=portal_arn,
            userAccessLoggingSettingsArn=uals["userAccessLoggingSettingsArn"],
        )
        resp = client.disassociate_user_access_logging_settings(portalArn=portal_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkspaceswebIpAccessSettings:
    """Tests for IpAccessSettings CRUD and associations."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces-web")

    def test_create_ip_access_settings(self, client):
        """CreateIpAccessSettings returns an ARN."""
        resp = client.create_ip_access_settings(
            displayName="Test IAS",
            ipRules=[{"ipRange": "10.0.0.0/8", "description": "corp"}],
        )
        assert "ipAccessSettingsArn" in resp
        assert resp["ipAccessSettingsArn"].startswith("arn:aws:workspaces-web:")

    def test_get_ip_access_settings(self, client):
        """GetIpAccessSettings returns the settings."""
        create_resp = client.create_ip_access_settings(
            displayName="Test IAS",
            ipRules=[{"ipRange": "10.0.0.0/8", "description": "corp"}],
        )
        arn = create_resp["ipAccessSettingsArn"]
        resp = client.get_ip_access_settings(ipAccessSettingsArn=arn)
        assert "ipAccessSettings" in resp
        assert resp["ipAccessSettings"]["ipAccessSettingsArn"] == arn
        assert resp["ipAccessSettings"]["displayName"] == "Test IAS"

    def test_list_ip_access_settings(self, client):
        """ListIpAccessSettings returns a list."""
        client.create_ip_access_settings(
            displayName="Test IAS",
            ipRules=[{"ipRange": "10.0.0.0/8", "description": "corp"}],
        )
        resp = client.list_ip_access_settings()
        assert "ipAccessSettings" in resp
        assert isinstance(resp["ipAccessSettings"], list)
        assert len(resp["ipAccessSettings"]) >= 1

    def test_update_ip_access_settings(self, client):
        """UpdateIpAccessSettings returns updated settings."""
        create_resp = client.create_ip_access_settings(
            displayName="Original",
            ipRules=[{"ipRange": "10.0.0.0/8", "description": "corp"}],
        )
        arn = create_resp["ipAccessSettingsArn"]
        resp = client.update_ip_access_settings(ipAccessSettingsArn=arn, displayName="Updated")
        assert "ipAccessSettings" in resp
        assert resp["ipAccessSettings"]["displayName"] == "Updated"

    def test_delete_ip_access_settings(self, client):
        """DeleteIpAccessSettings removes the settings."""
        create_resp = client.create_ip_access_settings(
            displayName="ToDelete",
            ipRules=[{"ipRange": "10.0.0.0/8", "description": "corp"}],
        )
        arn = create_resp["ipAccessSettingsArn"]
        resp = client.delete_ip_access_settings(ipAccessSettingsArn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_associate_and_disassociate_ip_access_settings(self, client):
        """Associate/Disassociate IpAccessSettings with portal."""
        portal_arn = client.create_portal(displayName="P")["portalArn"]
        ias_arn = client.create_ip_access_settings(
            displayName="IAS",
            ipRules=[{"ipRange": "10.0.0.0/8", "description": "corp"}],
        )["ipAccessSettingsArn"]
        assoc = client.associate_ip_access_settings(
            portalArn=portal_arn, ipAccessSettingsArn=ias_arn
        )
        assert assoc["portalArn"] == portal_arn
        assert assoc["ipAccessSettingsArn"] == ias_arn
        disassoc = client.disassociate_ip_access_settings(portalArn=portal_arn)
        assert disassoc["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkspaceswebTrustStore:
    """Tests for TrustStore CRUD and associations."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces-web")

    @pytest.fixture
    def cert(self):
        import base64

        return base64.b64encode(b"fake-certificate-data").decode()

    def test_create_trust_store(self, client, cert):
        """CreateTrustStore returns an ARN."""
        resp = client.create_trust_store(certificateList=[cert])
        assert "trustStoreArn" in resp
        assert resp["trustStoreArn"].startswith("arn:aws:workspaces-web:")

    def test_get_trust_store(self, client, cert):
        """GetTrustStore returns the trust store."""
        create_resp = client.create_trust_store(certificateList=[cert])
        arn = create_resp["trustStoreArn"]
        resp = client.get_trust_store(trustStoreArn=arn)
        assert "trustStore" in resp
        assert resp["trustStore"]["trustStoreArn"] == arn

    def test_list_trust_stores(self, client, cert):
        """ListTrustStores returns a list."""
        client.create_trust_store(certificateList=[cert])
        resp = client.list_trust_stores()
        assert "trustStores" in resp
        assert isinstance(resp["trustStores"], list)
        assert len(resp["trustStores"]) >= 1

    def test_list_trust_store_certificates(self, client, cert):
        """ListTrustStoreCertificates returns certificate list."""
        create_resp = client.create_trust_store(certificateList=[cert])
        arn = create_resp["trustStoreArn"]
        resp = client.list_trust_store_certificates(trustStoreArn=arn)
        assert "certificateList" in resp
        assert "trustStoreArn" in resp
        assert resp["trustStoreArn"] == arn

    def test_update_trust_store(self, client, cert):
        """UpdateTrustStore returns the trust store ARN."""
        import base64

        cert2 = base64.b64encode(b"another-cert").decode()
        create_resp = client.create_trust_store(certificateList=[cert])
        arn = create_resp["trustStoreArn"]
        resp = client.update_trust_store(trustStoreArn=arn, certificatesToAdd=[cert2])
        assert "trustStoreArn" in resp
        assert resp["trustStoreArn"] == arn

    def test_delete_trust_store(self, client, cert):
        """DeleteTrustStore removes the trust store."""
        create_resp = client.create_trust_store(certificateList=[cert])
        arn = create_resp["trustStoreArn"]
        resp = client.delete_trust_store(trustStoreArn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_associate_and_disassociate_trust_store(self, client, cert):
        """Associate/Disassociate TrustStore with portal."""
        portal_arn = client.create_portal(displayName="P")["portalArn"]
        ts_arn = client.create_trust_store(certificateList=[cert])["trustStoreArn"]
        assoc = client.associate_trust_store(portalArn=portal_arn, trustStoreArn=ts_arn)
        assert assoc["portalArn"] == portal_arn
        assert assoc["trustStoreArn"] == ts_arn
        disassoc = client.disassociate_trust_store(portalArn=portal_arn)
        assert disassoc["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkspaceswebIdentityProvider:
    """Tests for IdentityProvider CRUD."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces-web")

    @pytest.fixture
    def portal_arn(self, client):
        return client.create_portal(displayName="Test Portal")["portalArn"]

    def test_create_identity_provider(self, client, portal_arn):
        """CreateIdentityProvider returns an ARN."""
        resp = client.create_identity_provider(
            portalArn=portal_arn,
            identityProviderName="TestIdP",
            identityProviderType="SAML",
            identityProviderDetails={"MetadataURL": "https://idp.example.com/metadata"},
        )
        assert "identityProviderArn" in resp
        assert resp["identityProviderArn"].startswith("arn:aws:workspaces-web:")

    def test_get_identity_provider(self, client, portal_arn):
        """GetIdentityProvider returns the identity provider."""
        create_resp = client.create_identity_provider(
            portalArn=portal_arn,
            identityProviderName="TestIdP",
            identityProviderType="SAML",
            identityProviderDetails={"MetadataURL": "https://idp.example.com/metadata"},
        )
        arn = create_resp["identityProviderArn"]
        resp = client.get_identity_provider(identityProviderArn=arn)
        assert "identityProvider" in resp
        assert resp["identityProvider"]["identityProviderArn"] == arn
        assert resp["identityProvider"]["identityProviderName"] == "TestIdP"

    def test_list_identity_providers(self, client, portal_arn):
        """ListIdentityProviders returns a list for the portal."""
        client.create_identity_provider(
            portalArn=portal_arn,
            identityProviderName="TestIdP",
            identityProviderType="SAML",
            identityProviderDetails={"MetadataURL": "https://idp.example.com/metadata"},
        )
        resp = client.list_identity_providers(portalArn=portal_arn)
        assert "identityProviders" in resp
        assert isinstance(resp["identityProviders"], list)
        names = [idp["identityProviderName"] for idp in resp["identityProviders"]]
        assert "TestIdP" in names

    def test_update_identity_provider(self, client, portal_arn):
        """UpdateIdentityProvider returns updated identity provider."""
        create_resp = client.create_identity_provider(
            portalArn=portal_arn,
            identityProviderName="Original",
            identityProviderType="SAML",
            identityProviderDetails={"MetadataURL": "https://idp.example.com/metadata"},
        )
        arn = create_resp["identityProviderArn"]
        resp = client.update_identity_provider(
            identityProviderArn=arn, identityProviderName="Updated"
        )
        assert "identityProvider" in resp
        assert resp["identityProvider"]["identityProviderName"] == "Updated"

    def test_delete_identity_provider(self, client, portal_arn):
        """DeleteIdentityProvider removes the identity provider."""
        create_resp = client.create_identity_provider(
            portalArn=portal_arn,
            identityProviderName="ToDelete",
            identityProviderType="SAML",
            identityProviderDetails={"MetadataURL": "https://idp.example.com/metadata"},
        )
        arn = create_resp["identityProviderArn"]
        resp = client.delete_identity_provider(identityProviderArn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkspaceswebDataProtectionSettings:
    """Tests for DataProtectionSettings CRUD and associations."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces-web")

    def test_create_data_protection_settings(self, client):
        """CreateDataProtectionSettings returns an ARN."""
        resp = client.create_data_protection_settings(displayName="Test DPS")
        assert "dataProtectionSettingsArn" in resp
        assert resp["dataProtectionSettingsArn"].startswith("arn:aws:workspaces-web:")

    def test_get_data_protection_settings(self, client):
        """GetDataProtectionSettings returns the settings."""
        create_resp = client.create_data_protection_settings(displayName="Test DPS")
        arn = create_resp["dataProtectionSettingsArn"]
        resp = client.get_data_protection_settings(dataProtectionSettingsArn=arn)
        assert "dataProtectionSettings" in resp
        assert resp["dataProtectionSettings"]["dataProtectionSettingsArn"] == arn
        assert resp["dataProtectionSettings"]["displayName"] == "Test DPS"

    def test_list_data_protection_settings(self, client):
        """ListDataProtectionSettings returns a list."""
        client.create_data_protection_settings(displayName="DPS1")
        resp = client.list_data_protection_settings()
        assert "dataProtectionSettings" in resp
        assert isinstance(resp["dataProtectionSettings"], list)
        assert len(resp["dataProtectionSettings"]) >= 1

    def test_update_data_protection_settings(self, client):
        """UpdateDataProtectionSettings returns updated settings."""
        create_resp = client.create_data_protection_settings(displayName="Original")
        arn = create_resp["dataProtectionSettingsArn"]
        resp = client.update_data_protection_settings(
            dataProtectionSettingsArn=arn, displayName="Updated"
        )
        assert "dataProtectionSettings" in resp
        assert resp["dataProtectionSettings"]["displayName"] == "Updated"

    def test_delete_data_protection_settings(self, client):
        """DeleteDataProtectionSettings removes the settings."""
        create_resp = client.create_data_protection_settings(displayName="ToDelete")
        arn = create_resp["dataProtectionSettingsArn"]
        resp = client.delete_data_protection_settings(dataProtectionSettingsArn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_associate_and_disassociate_data_protection_settings(self, client):
        """Associate/Disassociate DataProtectionSettings with portal."""
        portal_arn = client.create_portal(displayName="P")["portalArn"]
        dps_arn = client.create_data_protection_settings(displayName="DPS")[
            "dataProtectionSettingsArn"
        ]
        assoc = client.associate_data_protection_settings(
            portalArn=portal_arn, dataProtectionSettingsArn=dps_arn
        )
        assert assoc["portalArn"] == portal_arn
        assert assoc["dataProtectionSettingsArn"] == dps_arn
        disassoc = client.disassociate_data_protection_settings(portalArn=portal_arn)
        assert disassoc["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkspaceswebSessionLogger:
    """Tests for SessionLogger CRUD and associations."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces-web")

    @pytest.fixture
    def log_config(self):
        return {
            "s3": {
                "bucket": "my-log-bucket",
                "logFileFormat": "JSON",
                "folderStructure": "FLAT",
            }
        }

    @pytest.fixture
    def event_filter(self):
        return {"all": {}}

    def test_create_session_logger(self, client, log_config, event_filter):
        """CreateSessionLogger returns an ARN."""
        resp = client.create_session_logger(
            displayName="Test SL",
            eventFilter=event_filter,
            logConfiguration=log_config,
        )
        assert "sessionLoggerArn" in resp
        assert resp["sessionLoggerArn"].startswith("arn:aws:workspaces-web:")

    def test_get_session_logger(self, client, log_config, event_filter):
        """GetSessionLogger returns the session logger."""
        create_resp = client.create_session_logger(
            displayName="Test SL",
            eventFilter=event_filter,
            logConfiguration=log_config,
        )
        arn = create_resp["sessionLoggerArn"]
        resp = client.get_session_logger(sessionLoggerArn=arn)
        assert "sessionLogger" in resp
        assert resp["sessionLogger"]["sessionLoggerArn"] == arn
        assert resp["sessionLogger"]["displayName"] == "Test SL"

    def test_list_session_loggers(self, client, log_config, event_filter):
        """ListSessionLoggers returns a list."""
        client.create_session_logger(
            displayName="SL1", eventFilter=event_filter, logConfiguration=log_config
        )
        resp = client.list_session_loggers()
        assert "sessionLoggers" in resp
        assert isinstance(resp["sessionLoggers"], list)
        assert len(resp["sessionLoggers"]) >= 1

    def test_update_session_logger(self, client, log_config, event_filter):
        """UpdateSessionLogger returns updated session logger."""
        create_resp = client.create_session_logger(
            displayName="Original", eventFilter=event_filter, logConfiguration=log_config
        )
        arn = create_resp["sessionLoggerArn"]
        resp = client.update_session_logger(sessionLoggerArn=arn, displayName="Updated")
        assert "sessionLogger" in resp
        assert resp["sessionLogger"]["displayName"] == "Updated"

    def test_delete_session_logger(self, client, log_config, event_filter):
        """DeleteSessionLogger removes the session logger."""
        create_resp = client.create_session_logger(
            displayName="ToDelete",
            eventFilter=event_filter,
            logConfiguration=log_config,
        )
        arn = create_resp["sessionLoggerArn"]
        resp = client.delete_session_logger(sessionLoggerArn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_associate_and_disassociate_session_logger(self, client, log_config, event_filter):
        """Associate/Disassociate SessionLogger with portal."""
        portal_arn = client.create_portal(displayName="P")["portalArn"]
        sl_arn = client.create_session_logger(
            displayName="SL",
            eventFilter=event_filter,
            logConfiguration=log_config,
        )["sessionLoggerArn"]
        assoc = client.associate_session_logger(portalArn=portal_arn, sessionLoggerArn=sl_arn)
        assert assoc["portalArn"] == portal_arn
        assert assoc["sessionLoggerArn"] == sl_arn
        disassoc = client.disassociate_session_logger(portalArn=portal_arn)
        assert disassoc["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkSpacesWebTrustStoreCertificates:
    """Tests for trust store certificate operations."""

    def test_get_trust_store_certificate_not_found(self, workspacesweb):
        """GetTrustStoreCertificate raises ResourceNotFoundException for missing trust store."""
        with pytest.raises(ClientError) as exc:
            workspacesweb.get_trust_store_certificate(
                trustStoreArn="arn:aws:workspaces-web:us-east-1:123456789012:trustStore/nonexistent",
                thumbprint="a" * 64,
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
