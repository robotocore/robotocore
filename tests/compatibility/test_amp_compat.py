"""Amazon Managed Prometheus (AMP) compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def amp():
    return make_client("amp")


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
