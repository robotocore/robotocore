"""Amazon Managed Prometheus (AMP) compatibility tests."""

import base64
import uuid

import pytest

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
