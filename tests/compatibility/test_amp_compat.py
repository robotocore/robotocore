"""Amazon Managed Prometheus (AMP) compatibility tests."""

import base64
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def amp():
    return make_client("amp")


@pytest.fixture
def workspace(amp):
    """Create a workspace for tests that need one, clean up after."""
    alias = f"test-ws-fix-{uuid.uuid4().hex[:8]}"
    resp = amp.create_workspace(alias=alias)
    ws_id = resp["workspaceId"]
    yield ws_id, resp["arn"]
    amp.delete_workspace(workspaceId=ws_id)


class TestAMPOperations:
    def test_list_workspaces_empty(self, amp):
        """ListWorkspaces returns a workspaces list."""
        response = amp.list_workspaces()
        assert "workspaces" in response

    def test_create_workspace(self, amp):
        """CreateWorkspace returns workspaceId, arn, status, and tags."""
        alias = f"test-ws-{uuid.uuid4().hex[:8]}"
        response = amp.create_workspace(alias=alias)
        assert "workspaceId" in response
        assert "arn" in response
        assert "status" in response
        assert "tags" in response
        # Clean up
        amp.delete_workspace(workspaceId=response["workspaceId"])

    def test_create_workspace_with_tags(self, amp):
        """CreateWorkspace with tags returns them back."""
        alias = f"test-ws-tags-{uuid.uuid4().hex[:8]}"
        tags = {"env": "test", "project": "robotocore"}
        response = amp.create_workspace(alias=alias, tags=tags)
        assert response["tags"] == tags
        amp.delete_workspace(workspaceId=response["workspaceId"])

    def test_create_workspace_status(self, amp):
        """CreateWorkspace returns a status with a statusCode."""
        alias = f"test-ws-status-{uuid.uuid4().hex[:8]}"
        response = amp.create_workspace(alias=alias)
        assert "statusCode" in response["status"]
        amp.delete_workspace(workspaceId=response["workspaceId"])

    def test_describe_workspace(self, amp):
        """DescribeWorkspace returns workspace details."""
        alias = f"test-ws-desc-{uuid.uuid4().hex[:8]}"
        create_resp = amp.create_workspace(alias=alias)
        workspace_id = create_resp["workspaceId"]

        response = amp.describe_workspace(workspaceId=workspace_id)
        assert "workspace" in response
        ws = response["workspace"]
        assert ws["workspaceId"] == workspace_id
        assert ws["arn"] == create_resp["arn"]
        assert "status" in ws
        # Clean up
        amp.delete_workspace(workspaceId=workspace_id)

    def test_describe_workspace_has_alias(self, amp):
        """DescribeWorkspace includes the alias that was set at creation."""
        alias = f"test-ws-alias-{uuid.uuid4().hex[:8]}"
        create_resp = amp.create_workspace(alias=alias)
        workspace_id = create_resp["workspaceId"]

        response = amp.describe_workspace(workspaceId=workspace_id)
        assert response["workspace"].get("alias") == alias
        amp.delete_workspace(workspaceId=workspace_id)

    def test_list_workspaces_includes_created(self, amp):
        """ListWorkspaces includes a workspace that was just created."""
        alias = f"test-ws-list-{uuid.uuid4().hex[:8]}"
        create_resp = amp.create_workspace(alias=alias)
        workspace_id = create_resp["workspaceId"]

        response = amp.list_workspaces()
        workspace_ids = [ws["workspaceId"] for ws in response["workspaces"]]
        assert workspace_id in workspace_ids
        amp.delete_workspace(workspaceId=workspace_id)

    def test_delete_workspace(self, amp):
        """DeleteWorkspace removes the workspace from list results."""
        alias = f"test-ws-del-{uuid.uuid4().hex[:8]}"
        create_resp = amp.create_workspace(alias=alias)
        workspace_id = create_resp["workspaceId"]

        amp.delete_workspace(workspaceId=workspace_id)

        response = amp.list_workspaces()
        workspace_ids = [
            ws["workspaceId"]
            for ws in response["workspaces"]
            if ws.get("status", {}).get("statusCode") not in ("DELETING",)
        ]
        assert workspace_id not in workspace_ids

    def test_create_multiple_workspaces(self, amp):
        """Multiple workspaces can be created and listed."""
        alias1 = f"test-ws-multi1-{uuid.uuid4().hex[:8]}"
        alias2 = f"test-ws-multi2-{uuid.uuid4().hex[:8]}"
        resp1 = amp.create_workspace(alias=alias1)
        resp2 = amp.create_workspace(alias=alias2)

        response = amp.list_workspaces()
        workspace_ids = [ws["workspaceId"] for ws in response["workspaces"]]
        assert resp1["workspaceId"] in workspace_ids
        assert resp2["workspaceId"] in workspace_ids

        amp.delete_workspace(workspaceId=resp1["workspaceId"])
        amp.delete_workspace(workspaceId=resp2["workspaceId"])

    def test_describe_workspace_not_found(self, amp):
        """DescribeWorkspace with a fake ID returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            amp.describe_workspace(workspaceId="ws-fake-00000000")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_workspace_alias(self, amp):
        """UpdateWorkspaceAlias changes the alias on an existing workspace."""
        alias = f"test-ws-upd-{uuid.uuid4().hex[:8]}"
        resp = amp.create_workspace(alias=alias)
        ws_id = resp["workspaceId"]
        try:
            new_alias = f"test-ws-new-{uuid.uuid4().hex[:8]}"
            amp.update_workspace_alias(workspaceId=ws_id, alias=new_alias)
            desc = amp.describe_workspace(workspaceId=ws_id)
            assert desc["workspace"]["alias"] == new_alias
        finally:
            amp.delete_workspace(workspaceId=ws_id)

    def test_list_workspaces_alias_filter(self, amp):
        """ListWorkspaces with alias filter returns only matching workspaces."""
        alias = f"test-ws-filt-{uuid.uuid4().hex[:8]}"
        resp = amp.create_workspace(alias=alias)
        ws_id = resp["workspaceId"]
        try:
            filtered = amp.list_workspaces(alias=alias)
            assert len(filtered["workspaces"]) >= 1
            assert all(ws.get("alias") == alias for ws in filtered["workspaces"])
        finally:
            amp.delete_workspace(workspaceId=ws_id)

    def test_list_workspaces_max_results(self, amp):
        """ListWorkspaces with maxResults limits results."""
        resp = amp.list_workspaces(maxResults=1)
        assert "workspaces" in resp
        assert len(resp["workspaces"]) <= 1

    def test_get_default_scraper_configuration(self, amp):
        """GetDefaultScraperConfiguration returns a configuration blob."""
        resp = amp.get_default_scraper_configuration()
        assert "configuration" in resp

    def test_list_scrapers(self, amp):
        """ListScrapers returns a scrapers list."""
        resp = amp.list_scrapers()
        assert "scrapers" in resp


class TestAMPScrapers:
    def test_create_scraper(self, amp, workspace):
        """CreateScraper returns scraperId, arn, and status."""
        ws_id, ws_arn = workspace
        resp = amp.create_scraper(
            alias="test-scraper",
            scrapeConfiguration={"configurationBlob": base64.b64encode(b"scrape_configs: []")},
            source={
                "eksConfiguration": {
                    "clusterArn": "arn:aws:eks:us-east-1:123456789012:cluster/test",
                    "subnetIds": ["subnet-12345"],
                }
            },
            destination={"ampConfiguration": {"workspaceArn": ws_arn}},
        )
        assert "scraperId" in resp
        assert "arn" in resp
        assert "status" in resp
        amp.delete_scraper(scraperId=resp["scraperId"])

    def test_describe_scraper(self, amp, workspace):
        """DescribeScraper returns scraper details after creation."""
        ws_id, ws_arn = workspace
        create_resp = amp.create_scraper(
            alias="test-scraper-desc",
            scrapeConfiguration={"configurationBlob": base64.b64encode(b"scrape_configs: []")},
            source={
                "eksConfiguration": {
                    "clusterArn": "arn:aws:eks:us-east-1:123456789012:cluster/test",
                    "subnetIds": ["subnet-12345"],
                }
            },
            destination={"ampConfiguration": {"workspaceArn": ws_arn}},
        )
        scraper_id = create_resp["scraperId"]
        try:
            resp = amp.describe_scraper(scraperId=scraper_id)
            assert "scraper" in resp
            s = resp["scraper"]
            assert s["scraperId"] == scraper_id
            assert "arn" in s
            assert "status" in s
            assert s["alias"] == "test-scraper-desc"
        finally:
            amp.delete_scraper(scraperId=scraper_id)

    def test_list_scrapers_includes_created(self, amp, workspace):
        """ListScrapers includes a scraper that was just created."""
        ws_id, ws_arn = workspace
        create_resp = amp.create_scraper(
            alias="test-scraper-list",
            scrapeConfiguration={"configurationBlob": base64.b64encode(b"scrape_configs: []")},
            source={
                "eksConfiguration": {
                    "clusterArn": "arn:aws:eks:us-east-1:123456789012:cluster/test",
                    "subnetIds": ["subnet-12345"],
                }
            },
            destination={"ampConfiguration": {"workspaceArn": ws_arn}},
        )
        scraper_id = create_resp["scraperId"]
        try:
            resp = amp.list_scrapers()
            scraper_ids = [s["scraperId"] for s in resp["scrapers"]]
            assert scraper_id in scraper_ids
        finally:
            amp.delete_scraper(scraperId=scraper_id)

    def test_delete_scraper(self, amp, workspace):
        """DeleteScraper removes the scraper."""
        ws_id, ws_arn = workspace
        create_resp = amp.create_scraper(
            alias="test-scraper-del",
            scrapeConfiguration={"configurationBlob": base64.b64encode(b"scrape_configs: []")},
            source={
                "eksConfiguration": {
                    "clusterArn": "arn:aws:eks:us-east-1:123456789012:cluster/test",
                    "subnetIds": ["subnet-12345"],
                }
            },
            destination={"ampConfiguration": {"workspaceArn": ws_arn}},
        )
        scraper_id = create_resp["scraperId"]
        resp = amp.delete_scraper(scraperId=scraper_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 202)

    def test_describe_scraper_not_found(self, amp):
        """DescribeScraper with a fake ID returns 404."""
        with pytest.raises(ClientError) as exc_info:
            amp.describe_scraper(scraperId="s-fake-00000000-0000-0000-0000-000000000000")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


class TestAMPAlertManagerDefinition:
    def test_create_alert_manager_definition(self, amp, workspace):
        """CreateAlertManagerDefinition returns a status."""
        ws_id, _ = workspace
        data = base64.b64encode(b"alertmanager_config: test").decode()
        resp = amp.create_alert_manager_definition(workspaceId=ws_id, data=data)
        assert "status" in resp
        assert resp["status"]["statusCode"] in ("CREATING", "ACTIVE")

    def test_describe_alert_manager_definition(self, amp, workspace):
        """DescribeAlertManagerDefinition returns the definition after creation."""
        ws_id, _ = workspace
        data = base64.b64encode(b"alertmanager_config: test").decode()
        amp.create_alert_manager_definition(workspaceId=ws_id, data=data)
        resp = amp.describe_alert_manager_definition(workspaceId=ws_id)
        assert "alertManagerDefinition" in resp
        amd = resp["alertManagerDefinition"]
        assert "data" in amd
        assert "status" in amd

    def test_put_alert_manager_definition(self, amp, workspace):
        """PutAlertManagerDefinition updates the definition."""
        ws_id, _ = workspace
        data = base64.b64encode(b"alertmanager_config: original").decode()
        amp.create_alert_manager_definition(workspaceId=ws_id, data=data)
        new_data = base64.b64encode(b"alertmanager_config: updated").decode()
        resp = amp.put_alert_manager_definition(workspaceId=ws_id, data=new_data)
        assert "status" in resp
        assert resp["status"]["statusCode"] in ("UPDATING", "ACTIVE")

    def test_delete_alert_manager_definition(self, amp, workspace):
        """DeleteAlertManagerDefinition succeeds after creation."""
        ws_id, _ = workspace
        data = base64.b64encode(b"alertmanager_config: test").decode()
        amp.create_alert_manager_definition(workspaceId=ws_id, data=data)
        resp = amp.delete_alert_manager_definition(workspaceId=ws_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 202)


class TestAMPLoggingConfiguration:
    def test_create_logging_configuration(self, amp, workspace):
        ws_id, _ = workspace
        log_group_arn = "arn:aws:logs:us-east-1:123456789012:log-group:test-amp-logs"
        resp = amp.create_logging_configuration(workspaceId=ws_id, logGroupArn=log_group_arn)
        assert "status" in resp
        assert resp["status"]["statusCode"] in ("CREATING", "ACTIVE")

    def test_describe_logging_configuration(self, amp, workspace):
        ws_id, _ = workspace
        log_group_arn = "arn:aws:logs:us-east-1:123456789012:log-group:test-amp-desc"
        amp.create_logging_configuration(workspaceId=ws_id, logGroupArn=log_group_arn)
        resp = amp.describe_logging_configuration(workspaceId=ws_id)
        assert "loggingConfiguration" in resp
        lc = resp["loggingConfiguration"]
        assert lc["logGroupArn"] == log_group_arn
        assert lc["workspace"] == ws_id

    def test_update_logging_configuration(self, amp, workspace):
        ws_id, _ = workspace
        log_group_arn = "arn:aws:logs:us-east-1:123456789012:log-group:test-amp-upd1"
        amp.create_logging_configuration(workspaceId=ws_id, logGroupArn=log_group_arn)
        new_arn = "arn:aws:logs:us-east-1:123456789012:log-group:test-amp-upd2"
        resp = amp.update_logging_configuration(workspaceId=ws_id, logGroupArn=new_arn)
        assert "status" in resp
        # Verify the update took effect
        desc = amp.describe_logging_configuration(workspaceId=ws_id)
        assert desc["loggingConfiguration"]["logGroupArn"] == new_arn

    def test_delete_logging_configuration(self, amp, workspace):
        ws_id, _ = workspace
        log_group_arn = "arn:aws:logs:us-east-1:123456789012:log-group:test-amp-del"
        amp.create_logging_configuration(workspaceId=ws_id, logGroupArn=log_group_arn)
        resp = amp.delete_logging_configuration(workspaceId=ws_id)
        # Delete should succeed (2xx response)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 202)

    def test_describe_logging_configuration_not_found(self, amp):
        """DescribeLoggingConfiguration with fake workspace returns error."""
        with pytest.raises(ClientError) as exc_info:
            amp.describe_logging_configuration(workspaceId="ws-fake-00000000")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestAMPRuleGroupsNamespace:
    def test_create_rule_groups_namespace(self, amp, workspace):
        ws_id, _ = workspace
        name = f"test-rg-{uuid.uuid4().hex[:8]}"
        data = base64.b64encode(b"groups:\n  - name: test\n    rules: []\n").decode()
        resp = amp.create_rule_groups_namespace(workspaceId=ws_id, name=name, data=data)
        assert "name" in resp
        assert resp["name"] == name
        assert "arn" in resp
        assert "status" in resp
        amp.delete_rule_groups_namespace(workspaceId=ws_id, name=name)

    def test_describe_rule_groups_namespace(self, amp, workspace):
        ws_id, _ = workspace
        name = f"test-rg-desc-{uuid.uuid4().hex[:8]}"
        data = base64.b64encode(b"groups:\n  - name: test\n    rules: []\n").decode()
        amp.create_rule_groups_namespace(workspaceId=ws_id, name=name, data=data)
        try:
            resp = amp.describe_rule_groups_namespace(workspaceId=ws_id, name=name)
            assert "ruleGroupsNamespace" in resp
            ns = resp["ruleGroupsNamespace"]
            assert ns["name"] == name
            assert "arn" in ns
            assert "data" in ns
        finally:
            amp.delete_rule_groups_namespace(workspaceId=ws_id, name=name)

    def test_put_rule_groups_namespace(self, amp, workspace):
        ws_id, _ = workspace
        name = f"test-rg-put-{uuid.uuid4().hex[:8]}"
        data = base64.b64encode(b"groups:\n  - name: test\n    rules: []\n").decode()
        amp.create_rule_groups_namespace(workspaceId=ws_id, name=name, data=data)
        try:
            new_data = base64.b64encode(b"groups:\n  - name: updated\n    rules: []\n").decode()
            resp = amp.put_rule_groups_namespace(workspaceId=ws_id, name=name, data=new_data)
            assert "name" in resp
            assert resp["name"] == name
            assert "status" in resp
        finally:
            amp.delete_rule_groups_namespace(workspaceId=ws_id, name=name)

    def test_delete_rule_groups_namespace(self, amp, workspace):
        ws_id, _ = workspace
        name = f"test-rg-del-{uuid.uuid4().hex[:8]}"
        data = base64.b64encode(b"groups:\n  - name: test\n    rules: []\n").decode()
        amp.create_rule_groups_namespace(workspaceId=ws_id, name=name, data=data)
        amp.delete_rule_groups_namespace(workspaceId=ws_id, name=name)
        # Verify deletion
        resp = amp.list_rule_groups_namespaces(workspaceId=ws_id)
        names = [ns["name"] for ns in resp["ruleGroupsNamespaces"]]
        assert name not in names

    def test_list_rule_groups_namespaces(self, amp, workspace):
        ws_id, _ = workspace
        resp = amp.list_rule_groups_namespaces(workspaceId=ws_id)
        assert "ruleGroupsNamespaces" in resp

    def test_describe_rule_groups_namespace_not_found(self, amp):
        """DescribeRuleGroupsNamespace with fake workspace returns error."""
        with pytest.raises(ClientError) as exc_info:
            amp.describe_rule_groups_namespace(workspaceId="ws-fake-00000000", name="fake")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_rule_groups_namespaces_includes_created(self, amp, workspace):
        """ListRuleGroupsNamespaces includes a namespace that was just created."""
        ws_id, _ = workspace
        name = f"test-rg-lst-{uuid.uuid4().hex[:8]}"
        data = base64.b64encode(b"groups:\n  - name: test\n    rules: []\n").decode()
        amp.create_rule_groups_namespace(workspaceId=ws_id, name=name, data=data)
        try:
            resp = amp.list_rule_groups_namespaces(workspaceId=ws_id)
            names = [ns["name"] for ns in resp["ruleGroupsNamespaces"]]
            assert name in names
        finally:
            amp.delete_rule_groups_namespace(workspaceId=ws_id, name=name)


class TestAMPTagging:
    def test_tag_resource(self, amp, workspace):
        _, arn = workspace
        amp.tag_resource(resourceArn=arn, tags={"team": "platform"})
        resp = amp.list_tags_for_resource(resourceArn=arn)
        assert "tags" in resp
        assert resp["tags"].get("team") == "platform"

    def test_untag_resource(self, amp, workspace):
        _, arn = workspace
        amp.tag_resource(resourceArn=arn, tags={"team": "platform", "env": "test"})
        amp.untag_resource(resourceArn=arn, tagKeys=["team"])
        resp = amp.list_tags_for_resource(resourceArn=arn)
        assert "team" not in resp.get("tags", {})
        assert resp.get("tags", {}).get("env") == "test"

    def test_list_tags_for_resource(self, amp, workspace):
        _, arn = workspace
        resp = amp.list_tags_for_resource(resourceArn=arn)
        assert "tags" in resp

    def test_tag_resource_multiple_tags(self, amp, workspace):
        """TagResource can add multiple tags at once."""
        _, arn = workspace
        tags = {"key1": "val1", "key2": "val2", "key3": "val3"}
        amp.tag_resource(resourceArn=arn, tags=tags)
        resp = amp.list_tags_for_resource(resourceArn=arn)
        for k, v in tags.items():
            assert resp["tags"].get(k) == v

    def test_untag_resource_multiple_keys(self, amp, workspace):
        """UntagResource can remove multiple tags at once."""
        _, arn = workspace
        amp.tag_resource(resourceArn=arn, tags={"a": "1", "b": "2", "c": "3"})
        amp.untag_resource(resourceArn=arn, tagKeys=["a", "b"])
        resp = amp.list_tags_for_resource(resourceArn=arn)
        assert "a" not in resp.get("tags", {})
        assert "b" not in resp.get("tags", {})
        assert resp.get("tags", {}).get("c") == "3"
