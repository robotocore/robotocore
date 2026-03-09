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


class TestCodeBuildProjectEdgeCases:
    """Tests for CodeBuild project edge cases."""

    def test_create_project_with_description_and_tags(self, codebuild):
        """CreateProject preserves description, tags, and env vars."""
        name = _unique("project")
        try:
            resp = codebuild.create_project(
                name=name,
                description="My test project",
                source={"type": "S3", "location": "my-bucket/source.zip"},
                artifacts={"type": "NO_ARTIFACTS"},
                environment={
                    "type": "LINUX_CONTAINER",
                    "image": "aws/codebuild/standard:5.0",
                    "computeType": "BUILD_GENERAL1_SMALL",
                    "environmentVariables": [
                        {"name": "MY_VAR", "value": "my_val", "type": "PLAINTEXT"}
                    ],
                },
                serviceRole="arn:aws:iam::123456789012:role/codebuild-role",
                tags=[{"key": "Env", "value": "test"}],
            )
            project = resp["project"]
            assert project["description"] == "My test project"
            assert project["tags"] == [{"key": "Env", "value": "test"}]
            env_vars = project["environment"]["environmentVariables"]
            assert len(env_vars) == 1
            assert env_vars[0]["name"] == "MY_VAR"
            assert env_vars[0]["value"] == "my_val"
        finally:
            codebuild.delete_project(name=name)

    def test_create_project_with_github_source(self, codebuild):
        """CreateProject works with GITHUB source type."""
        name = _unique("project")
        try:
            resp = codebuild.create_project(
                name=name,
                source={"type": "GITHUB", "location": "https://github.com/example/repo.git"},
                artifacts={"type": "S3", "location": "my-output-bucket", "name": "output.zip"},
                environment={
                    "type": "LINUX_CONTAINER",
                    "image": "aws/codebuild/standard:5.0",
                    "computeType": "BUILD_GENERAL1_MEDIUM",
                },
                serviceRole="arn:aws:iam::123456789012:role/codebuild-role",
            )
            project = resp["project"]
            assert project["source"]["type"] == "GITHUB"
            assert project["artifacts"]["type"] == "S3"
            assert project["environment"]["computeType"] == "BUILD_GENERAL1_MEDIUM"
        finally:
            codebuild.delete_project(name=name)

    def test_create_duplicate_project_raises(self, codebuild):
        """CreateProject with duplicate name raises ResourceAlreadyExistsException."""
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
            with pytest.raises(Exception) as exc_info:
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
            assert "ResourceAlreadyExistsException" in str(exc_info.value)
        finally:
            codebuild.delete_project(name=name)

    def test_delete_nonexistent_project_succeeds(self, codebuild):
        """DeleteProject on nonexistent project does not raise."""
        resp = codebuild.delete_project(name="nonexistent-project-xyz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_batch_get_projects_nonexistent(self, codebuild):
        """BatchGetProjects with nonexistent names returns empty projects."""
        resp = codebuild.batch_get_projects(names=["nonexistent-project-xyz"])
        assert resp["projects"] == []

    def test_batch_get_projects_mixed(self, codebuild):
        """BatchGetProjects with mix of existing and nonexistent names."""
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
            resp = codebuild.batch_get_projects(names=[name, "nonexistent-xyz"])
            assert len(resp["projects"]) == 1
            assert resp["projects"][0]["name"] == name
        finally:
            codebuild.delete_project(name=name)

    def test_list_projects_sorted(self, codebuild):
        """ListProjects with sortBy and sortOrder returns sorted list."""
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
            resp = codebuild.list_projects(sortBy="NAME", sortOrder="ASCENDING")
            assert isinstance(resp["projects"], list)
            assert name in resp["projects"]
        finally:
            codebuild.delete_project(name=name)

    def test_create_project_has_timestamps(self, codebuild):
        """CreateProject response includes created timestamp."""
        name = _unique("project")
        try:
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
            assert "created" in project
            assert project["created"] is not None
        finally:
            codebuild.delete_project(name=name)

    def test_batch_get_projects_after_delete(self, codebuild):
        """BatchGetProjects returns empty after project is deleted."""
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
        resp = codebuild.batch_get_projects(names=[name])
        assert resp["projects"] == []


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


class TestCodeBuildBuildEdgeCases:
    """Tests for CodeBuild build edge cases."""

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

    def test_start_build_nonexistent_project(self, codebuild):
        """StartBuild on nonexistent project raises ResourceNotFoundException."""
        with pytest.raises(Exception) as exc_info:
            codebuild.start_build(projectName="nonexistent-project-xyz")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_stop_build_nonexistent_id(self, codebuild):
        """StopBuild with nonexistent build ID raises ResourceNotFoundException."""
        with pytest.raises(Exception) as exc_info:
            codebuild.stop_build(id="nonexistent:bad-build-id")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_batch_get_builds_nonexistent(self, codebuild):
        """BatchGetBuilds with nonexistent IDs returns empty builds list."""
        resp = codebuild.batch_get_builds(ids=["nonexistent:build-id"])
        assert resp["builds"] == []

    def test_build_has_expected_fields(self, codebuild):
        """StartBuild response includes expected build fields."""
        name = self._create_project(codebuild)
        try:
            build = codebuild.start_build(projectName=name)["build"]
            assert "arn" in build
            assert "id" in build
            assert "projectName" in build
            assert "buildStatus" in build
            assert "startTime" in build
            assert "source" in build
            assert "environment" in build
            assert "serviceRole" in build
            assert "timeoutInMinutes" in build
            assert build["buildComplete"] is not None
        finally:
            codebuild.delete_project(name=name)

    def test_stop_build_sets_status_stopped(self, codebuild):
        """StopBuild sets buildStatus to STOPPED and currentPhase to COMPLETED."""
        name = self._create_project(codebuild)
        try:
            build_id = codebuild.start_build(projectName=name)["build"]["id"]
            resp = codebuild.stop_build(id=build_id)
            assert resp["build"]["buildStatus"] == "STOPPED"
            assert resp["build"]["currentPhase"] == "COMPLETED"
        finally:
            codebuild.delete_project(name=name)

    def test_list_builds_with_sort_order(self, codebuild):
        """ListBuilds accepts sortOrder parameter."""
        name = self._create_project(codebuild)
        try:
            codebuild.start_build(projectName=name)
            resp = codebuild.list_builds(sortOrder="ASCENDING")
            assert isinstance(resp.get("ids", []), list)
            assert len(resp["ids"]) >= 1
        finally:
            codebuild.delete_project(name=name)

    def test_build_number_increments(self, codebuild):
        """Each StartBuild increments the build number."""
        name = self._create_project(codebuild)
        try:
            b1 = codebuild.start_build(projectName=name)["build"]
            b2 = codebuild.start_build(projectName=name)["build"]
            assert b2["buildNumber"] > b1["buildNumber"]
        finally:
            codebuild.delete_project(name=name)

    def test_list_builds_for_project_nonexistent(self, codebuild):
        """ListBuildsForProject with nonexistent project raises ResourceNotFoundException."""
        with pytest.raises(Exception) as exc_info:
            codebuild.list_builds_for_project(projectName="nonexistent-project-xyz")
        assert "ResourceNotFoundException" in str(exc_info.value)


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
