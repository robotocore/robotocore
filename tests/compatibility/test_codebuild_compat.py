"""CodeBuild compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def codebuild():
    return make_client("codebuild")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestCodeBuildProjectOperations:
    """Tests for CodeBuild project CRUD operations."""

    def test_create_project(self, codebuild):
        name = _unique("project")
        resp = codebuild.create_project(
            name=name,
            source={"type": "S3", "location": "my-bucket/source.zip"},
            artifacts={"type": "NO_ARTIFACTS"},
            environment={
                "type": "LINUX_CONTAINER",
                "image": "aws/codebuild/standard:5.0",
                "computeType": "BUILD_GENERAL1_SMALL",
            },
            serviceRole="arn:aws:iam::123456789012:role/codebuild-role",
        )
        project = resp["project"]
        assert project["name"] == name
        assert project["arn"].endswith(f"project/{name}")
        assert project["source"]["type"] == "S3"
        assert project["environment"]["image"] == "aws/codebuild/standard:5.0"
        assert project["environment"]["computeType"] == "BUILD_GENERAL1_SMALL"
        # cleanup
        codebuild.delete_project(name=name)

    def test_batch_get_projects(self, codebuild):
        name = _unique("project")
        codebuild.create_project(
            name=name,
            source={"type": "S3", "location": "my-bucket/source.zip"},
            artifacts={"type": "NO_ARTIFACTS"},
            environment={
                "type": "LINUX_CONTAINER",
                "image": "aws/codebuild/standard:5.0",
                "computeType": "BUILD_GENERAL1_SMALL",
            },
            serviceRole="arn:aws:iam::123456789012:role/codebuild-role",
        )
        resp = codebuild.batch_get_projects(names=[name])
        assert len(resp["projects"]) == 1
        assert resp["projects"][0]["name"] == name
        # cleanup
        codebuild.delete_project(name=name)

    def test_list_projects(self, codebuild):
        name = _unique("project")
        codebuild.create_project(
            name=name,
            source={"type": "S3", "location": "my-bucket/source.zip"},
            artifacts={"type": "NO_ARTIFACTS"},
            environment={
                "type": "LINUX_CONTAINER",
                "image": "aws/codebuild/standard:5.0",
                "computeType": "BUILD_GENERAL1_SMALL",
            },
            serviceRole="arn:aws:iam::123456789012:role/codebuild-role",
        )
        resp = codebuild.list_projects()
        assert name in resp["projects"]
        # cleanup
        codebuild.delete_project(name=name)

    def test_delete_project(self, codebuild):
        name = _unique("project")
        codebuild.create_project(
            name=name,
            source={"type": "S3", "location": "my-bucket/source.zip"},
            artifacts={"type": "NO_ARTIFACTS"},
            environment={
                "type": "LINUX_CONTAINER",
                "image": "aws/codebuild/standard:5.0",
                "computeType": "BUILD_GENERAL1_SMALL",
            },
            serviceRole="arn:aws:iam::123456789012:role/codebuild-role",
        )
        codebuild.delete_project(name=name)
        resp = codebuild.list_projects()
        assert name not in resp.get("projects", [])

    def test_create_project_returns_arn(self, codebuild):
        name = _unique("project")
        resp = codebuild.create_project(
            name=name,
            source={"type": "S3", "location": "my-bucket/source.zip"},
            artifacts={"type": "NO_ARTIFACTS"},
            environment={
                "type": "LINUX_CONTAINER",
                "image": "aws/codebuild/standard:5.0",
                "computeType": "BUILD_GENERAL1_SMALL",
            },
            serviceRole="arn:aws:iam::123456789012:role/codebuild-role",
        )
        arn = resp["project"]["arn"]
        assert arn.startswith("arn:aws:codebuild:")
        assert f":project/{name}" in arn
        # cleanup
        codebuild.delete_project(name=name)


class TestCodeBuildBuildOperations:
    """Tests for CodeBuild build start/stop operations."""

    def _create_project(self, codebuild):
        name = _unique("project")
        codebuild.create_project(
            name=name,
            source={"type": "S3", "location": "my-bucket/source.zip"},
            artifacts={"type": "NO_ARTIFACTS"},
            environment={
                "type": "LINUX_CONTAINER",
                "image": "aws/codebuild/standard:5.0",
                "computeType": "BUILD_GENERAL1_SMALL",
            },
            serviceRole="arn:aws:iam::123456789012:role/codebuild-role",
        )
        return name

    def test_start_build(self, codebuild):
        name = self._create_project(codebuild)
        try:
            resp = codebuild.start_build(projectName=name)
            build = resp["build"]
            assert build["projectName"] == name
            assert "id" in build
            assert "arn" in build
            assert build["buildStatus"] in (
                "IN_PROGRESS",
                "SUCCEEDED",
                "FAILED",
                "STOPPED",
            )
        finally:
            codebuild.delete_project(name=name)

    def test_stop_build(self, codebuild):
        name = self._create_project(codebuild)
        try:
            start_resp = codebuild.start_build(projectName=name)
            build_id = start_resp["build"]["id"]
            stop_resp = codebuild.stop_build(id=build_id)
            assert "build" in stop_resp
            assert stop_resp["build"]["id"] == build_id
            assert stop_resp["build"]["buildStatus"] in ("STOPPED", "SUCCEEDED", "FAILED")
        finally:
            codebuild.delete_project(name=name)


class TestCodeBuildListOperations:
    """Tests for CodeBuild list operations that return empty results."""

    def test_list_builds_empty(self, codebuild):
        resp = codebuild.list_builds()
        # ids may be empty list or absent
        ids = resp.get("ids", [])
        assert isinstance(ids, list)

    def test_list_projects_returns_list(self, codebuild):
        resp = codebuild.list_projects()
        assert isinstance(resp["projects"], list)

    def test_list_builds_for_project(self, codebuild):
        """ListBuildsForProject returns build IDs for a project."""
        name = _unique("project")
        codebuild.create_project(
            name=name,
            source={"type": "S3", "location": "my-bucket/source.zip"},
            artifacts={"type": "NO_ARTIFACTS"},
            environment={
                "type": "LINUX_CONTAINER",
                "image": "aws/codebuild/standard:5.0",
                "computeType": "BUILD_GENERAL1_SMALL",
            },
            serviceRole="arn:aws:iam::123456789012:role/codebuild-role",
        )
        try:
            codebuild.start_build(projectName=name)
            resp = codebuild.list_builds_for_project(projectName=name)
            assert "ids" in resp
            assert isinstance(resp["ids"], list)
            assert len(resp["ids"]) >= 1
        finally:
            codebuild.delete_project(name=name)


class TestCodeBuildBatchGetBuilds:
    """Tests for BatchGetBuilds operation."""

    def _create_project(self, codebuild):
        name = _unique("project")
        codebuild.create_project(
            name=name,
            source={"type": "S3", "location": "my-bucket/source.zip"},
            artifacts={"type": "NO_ARTIFACTS"},
            environment={
                "type": "LINUX_CONTAINER",
                "image": "aws/codebuild/standard:5.0",
                "computeType": "BUILD_GENERAL1_SMALL",
            },
            serviceRole="arn:aws:iam::123456789012:role/codebuild-role",
        )
        return name

    def test_batch_get_builds(self, codebuild):
        """BatchGetBuilds returns build details."""
        name = self._create_project(codebuild)
        try:
            start_resp = codebuild.start_build(projectName=name)
            build_id = start_resp["build"]["id"]
            resp = codebuild.batch_get_builds(ids=[build_id])
            assert len(resp["builds"]) == 1
            assert resp["builds"][0]["id"] == build_id
            assert resp["builds"][0]["projectName"] == name
        finally:
            codebuild.delete_project(name=name)

    def test_batch_get_builds_multiple(self, codebuild):
        """BatchGetBuilds returns multiple builds."""
        name = self._create_project(codebuild)
        try:
            b1 = codebuild.start_build(projectName=name)["build"]["id"]
            b2 = codebuild.start_build(projectName=name)["build"]["id"]
            resp = codebuild.batch_get_builds(ids=[b1, b2])
            assert len(resp["builds"]) == 2
            returned_ids = {b["id"] for b in resp["builds"]}
            assert b1 in returned_ids
            assert b2 in returned_ids
        finally:
            codebuild.delete_project(name=name)

    def test_list_builds_after_start(self, codebuild):
        """ListBuilds includes a build after it's started."""
        name = self._create_project(codebuild)
        try:
            start_resp = codebuild.start_build(projectName=name)
            build_id = start_resp["build"]["id"]
            resp = codebuild.list_builds()
            assert build_id in resp.get("ids", [])
        finally:
            codebuild.delete_project(name=name)
