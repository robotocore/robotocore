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


class TestCodeBuildReportGroupOperations:
    """Tests for CodeBuild report group CRUD operations."""

    def test_create_report_group(self, codebuild):
        """CreateReportGroup creates a report group and returns its details."""
        name = _unique("rg")
        try:
            resp = codebuild.create_report_group(
                name=name,
                type="TEST",
                exportConfig={"exportConfigType": "NO_EXPORT"},
            )
            rg = resp["reportGroup"]
            assert rg["name"] == name
            assert rg["type"] == "TEST"
            assert rg["arn"].endswith(f"report-group/{name}")
        finally:
            codebuild.delete_report_group(
                arn=f"arn:aws:codebuild:us-east-1:123456789012:report-group/{name}"
            )

    def test_list_report_groups(self, codebuild):
        """ListReportGroups returns ARNs of created report groups."""
        name = _unique("rg")
        try:
            codebuild.create_report_group(
                name=name,
                type="TEST",
                exportConfig={"exportConfigType": "NO_EXPORT"},
            )
            resp = codebuild.list_report_groups()
            arns = resp["reportGroups"]
            assert isinstance(arns, list)
            matching = [a for a in arns if name in a]
            assert len(matching) == 1
        finally:
            codebuild.delete_report_group(
                arn=f"arn:aws:codebuild:us-east-1:123456789012:report-group/{name}"
            )

    def test_batch_get_report_groups(self, codebuild):
        """BatchGetReportGroups returns details for report groups."""
        name = _unique("rg")
        arn = f"arn:aws:codebuild:us-east-1:123456789012:report-group/{name}"
        try:
            codebuild.create_report_group(
                name=name,
                type="TEST",
                exportConfig={"exportConfigType": "NO_EXPORT"},
            )
            resp = codebuild.batch_get_report_groups(reportGroupArns=[arn])
            assert len(resp["reportGroups"]) == 1
            assert resp["reportGroups"][0]["name"] == name
        finally:
            codebuild.delete_report_group(arn=arn)

    def test_update_report_group(self, codebuild):
        """UpdateReportGroup modifies a report group."""
        name = _unique("rg")
        arn = f"arn:aws:codebuild:us-east-1:123456789012:report-group/{name}"
        try:
            codebuild.create_report_group(
                name=name,
                type="TEST",
                exportConfig={"exportConfigType": "NO_EXPORT"},
            )
            resp = codebuild.update_report_group(
                arn=arn,
                exportConfig={"exportConfigType": "NO_EXPORT"},
            )
            assert resp["reportGroup"]["arn"] == arn
        finally:
            codebuild.delete_report_group(arn=arn)

    def test_delete_report_group(self, codebuild):
        """DeleteReportGroup removes a report group."""
        name = _unique("rg")
        arn = f"arn:aws:codebuild:us-east-1:123456789012:report-group/{name}"
        codebuild.create_report_group(
            name=name,
            type="TEST",
            exportConfig={"exportConfigType": "NO_EXPORT"},
        )
        resp = codebuild.delete_report_group(arn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it's gone
        groups = codebuild.list_report_groups()["reportGroups"]
        assert arn not in groups

    def test_batch_get_report_groups_nonexistent(self, codebuild):
        """BatchGetReportGroups with nonexistent ARN returns empty."""
        resp = codebuild.batch_get_report_groups(
            reportGroupArns=["arn:aws:codebuild:us-east-1:123456789012:report-group/nonexistent"]
        )
        assert resp["reportGroups"] == []


class TestCodeBuildSourceCredentialOperations:
    """Tests for CodeBuild source credential operations."""

    def test_import_source_credentials(self, codebuild):
        """ImportSourceCredentials stores credentials and returns ARN."""
        try:
            resp = codebuild.import_source_credentials(
                token="ghp_faketoken123",
                serverType="GITHUB",
                authType="PERSONAL_ACCESS_TOKEN",
                shouldOverwrite=True,
            )
            assert "arn" in resp
            assert "codebuild" in resp["arn"]
        finally:
            try:
                codebuild.delete_source_credentials(arn=resp["arn"])
            except Exception:
                pass

    def test_list_source_credentials(self, codebuild):
        """ListSourceCredentials returns stored credentials info."""
        try:
            imp = codebuild.import_source_credentials(
                token="ghp_listtest123",
                serverType="GITHUB",
                authType="PERSONAL_ACCESS_TOKEN",
                shouldOverwrite=True,
            )
            resp = codebuild.list_source_credentials()
            infos = resp["sourceCredentialsInfos"]
            assert isinstance(infos, list)
            assert len(infos) >= 1
            github_creds = [i for i in infos if i["serverType"] == "GITHUB"]
            assert len(github_creds) >= 1
        finally:
            try:
                codebuild.delete_source_credentials(arn=imp["arn"])
            except Exception:
                pass

    def test_delete_source_credentials(self, codebuild):
        """DeleteSourceCredentials removes stored credentials."""
        imp = codebuild.import_source_credentials(
            token="ghp_deltest123",
            serverType="BITBUCKET",
            authType="BASIC_AUTH",
            username="testuser",
        )
        arn = imp["arn"]
        resp = codebuild.delete_source_credentials(arn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestCodeBuildWebhookOperations:
    """Tests for CodeBuild webhook operations."""

    def _create_github_project(self, codebuild):
        name = _unique("wh-proj")
        codebuild.create_project(
            name=name,
            source={"type": "GITHUB", "location": "https://github.com/test/repo.git"},
            artifacts={"type": "NO_ARTIFACTS"},
            environment={
                "type": "LINUX_CONTAINER",
                "image": "aws/codebuild/standard:5.0",
                "computeType": "BUILD_GENERAL1_SMALL",
            },
            serviceRole="arn:aws:iam::123456789012:role/codebuild-role",
        )
        return name

    def test_create_webhook(self, codebuild):
        """CreateWebhook creates a webhook for a project."""
        name = self._create_github_project(codebuild)
        try:
            resp = codebuild.create_webhook(projectName=name)
            wh = resp["webhook"]
            assert "url" in wh
            assert "payloadUrl" in wh or "url" in wh
        finally:
            try:
                codebuild.delete_webhook(projectName=name)
            except Exception:
                pass
            codebuild.delete_project(name=name)

    def test_delete_webhook(self, codebuild):
        """DeleteWebhook removes the webhook from a project."""
        name = self._create_github_project(codebuild)
        try:
            codebuild.create_webhook(projectName=name)
            resp = codebuild.delete_webhook(projectName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            codebuild.delete_project(name=name)

    def test_update_webhook(self, codebuild):
        """UpdateWebhook modifies a project webhook."""
        name = self._create_github_project(codebuild)
        try:
            codebuild.create_webhook(projectName=name)
            resp = codebuild.update_webhook(projectName=name, rotateSecret=True)
            assert "webhook" in resp
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                codebuild.delete_webhook(projectName=name)
            except Exception:
                pass
            codebuild.delete_project(name=name)


class TestCodeBuildFleetOperations:
    """Tests for CodeBuild fleet operations."""

    def test_create_fleet(self, codebuild):
        """CreateFleet creates a fleet and returns its details."""
        name = _unique("fleet")
        fleet_arn = None
        try:
            resp = codebuild.create_fleet(
                name=name,
                baseCapacity=1,
                environmentType="LINUX_CONTAINER",
                computeType="BUILD_GENERAL1_SMALL",
            )
            fleet = resp["fleet"]
            fleet_arn = fleet["arn"]
            assert fleet["name"] == name
            assert fleet["baseCapacity"] == 1
            assert "arn" in fleet
        finally:
            if fleet_arn:
                codebuild.delete_fleet(arn=fleet_arn)

    def test_list_fleets(self, codebuild):
        """ListFleets returns ARNs of created fleets."""
        name = _unique("fleet")
        fleet_arn = None
        try:
            r = codebuild.create_fleet(
                name=name,
                baseCapacity=1,
                environmentType="LINUX_CONTAINER",
                computeType="BUILD_GENERAL1_SMALL",
            )
            fleet_arn = r["fleet"]["arn"]
            resp = codebuild.list_fleets()
            assert isinstance(resp["fleets"], list)
            assert fleet_arn in resp["fleets"]
        finally:
            if fleet_arn:
                codebuild.delete_fleet(arn=fleet_arn)

    def test_batch_get_fleets(self, codebuild):
        """BatchGetFleets returns details for given fleet names."""
        name = _unique("fleet")
        fleet_arn = None
        try:
            r = codebuild.create_fleet(
                name=name,
                baseCapacity=1,
                environmentType="LINUX_CONTAINER",
                computeType="BUILD_GENERAL1_SMALL",
            )
            fleet_arn = r["fleet"]["arn"]
            resp = codebuild.batch_get_fleets(names=[name])
            assert len(resp["fleets"]) == 1
            assert resp["fleets"][0]["name"] == name
        finally:
            if fleet_arn:
                codebuild.delete_fleet(arn=fleet_arn)

    def test_delete_fleet(self, codebuild):
        """DeleteFleet removes a fleet."""
        name = _unique("fleet")
        r = codebuild.create_fleet(
            name=name,
            baseCapacity=1,
            environmentType="LINUX_CONTAINER",
            computeType="BUILD_GENERAL1_SMALL",
        )
        fleet_arn = r["fleet"]["arn"]
        resp = codebuild.delete_fleet(arn=fleet_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify gone
        fleets = codebuild.list_fleets()["fleets"]
        assert fleet_arn not in fleets

    def test_update_fleet(self, codebuild):
        """UpdateFleet modifies fleet settings."""
        name = _unique("fleet")
        fleet_arn = None
        try:
            r = codebuild.create_fleet(
                name=name,
                baseCapacity=1,
                environmentType="LINUX_CONTAINER",
                computeType="BUILD_GENERAL1_SMALL",
            )
            fleet_arn = r["fleet"]["arn"]
            resp = codebuild.update_fleet(arn=fleet_arn, baseCapacity=2)
            assert resp["fleet"]["baseCapacity"] == 2
        finally:
            if fleet_arn:
                codebuild.delete_fleet(arn=fleet_arn)


class TestCodeBuildResourcePolicyOperations:
    """Tests for CodeBuild resource policy operations."""

    def test_put_resource_policy(self, codebuild):
        """PutResourcePolicy sets a policy on a resource."""
        resource_arn = "arn:aws:codebuild:us-east-1:123456789012:report-group/policy-test"
        try:
            resp = codebuild.put_resource_policy(
                policy='{"Version":"2012-10-17","Statement":[]}',
                resourceArn=resource_arn,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "resourceArn" in resp
        finally:
            try:
                codebuild.delete_resource_policy(resourceArn=resource_arn)
            except Exception:
                pass

    def test_get_resource_policy(self, codebuild):
        """GetResourcePolicy retrieves a resource policy."""
        resource_arn = "arn:aws:codebuild:us-east-1:123456789012:report-group/get-policy-test"
        try:
            codebuild.put_resource_policy(
                policy='{"Version":"2012-10-17","Statement":[]}',
                resourceArn=resource_arn,
            )
            resp = codebuild.get_resource_policy(resourceArn=resource_arn)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "policy" in resp
        finally:
            try:
                codebuild.delete_resource_policy(resourceArn=resource_arn)
            except Exception:
                pass

    def test_delete_resource_policy(self, codebuild):
        """DeleteResourcePolicy removes a resource policy."""
        resource_arn = "arn:aws:codebuild:us-east-1:123456789012:report-group/del-policy-test"
        codebuild.put_resource_policy(
            policy='{"Version":"2012-10-17","Statement":[]}',
            resourceArn=resource_arn,
        )
        resp = codebuild.delete_resource_policy(resourceArn=resource_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestCodeBuildMiscOperations:
    """Tests for miscellaneous CodeBuild operations."""

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

    def test_invalidate_project_cache(self, codebuild):
        """InvalidateProjectCache succeeds for a project."""
        name = self._create_project(codebuild)
        try:
            resp = codebuild.invalidate_project_cache(projectName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            codebuild.delete_project(name=name)

    def test_batch_delete_builds(self, codebuild):
        """BatchDeleteBuilds removes builds and returns deleted IDs."""
        name = self._create_project(codebuild)
        try:
            b = codebuild.start_build(projectName=name)
            build_id = b["build"]["id"]
            resp = codebuild.batch_delete_builds(ids=[build_id])
            assert build_id in resp.get("buildsDeleted", [])
            assert resp.get("buildsNotDeleted", []) == []
        finally:
            codebuild.delete_project(name=name)

    def test_update_project(self, codebuild):
        """UpdateProject modifies project settings."""
        name = self._create_project(codebuild)
        try:
            resp = codebuild.update_project(name=name, description="updated description")
            assert resp["project"]["description"] == "updated description"
            assert resp["project"]["name"] == name
        finally:
            codebuild.delete_project(name=name)

    def test_update_project_visibility(self, codebuild):
        """UpdateProjectVisibility changes project visibility."""
        name = self._create_project(codebuild)
        try:
            arn = f"arn:aws:codebuild:us-east-1:123456789012:project/{name}"
            resp = codebuild.update_project_visibility(projectArn=arn, projectVisibility="PRIVATE")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            codebuild.delete_project(name=name)

    def test_retry_build(self, codebuild):
        """RetryBuild retries a build and returns build details."""
        name = self._create_project(codebuild)
        try:
            b = codebuild.start_build(projectName=name)
            build_id = b["build"]["id"]
            resp = codebuild.retry_build(id=build_id)
            assert "build" in resp
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            codebuild.delete_project(name=name)

    def test_list_curated_environment_images(self, codebuild):
        """ListCuratedEnvironmentImages returns platform list."""
        resp = codebuild.list_curated_environment_images()
        assert "platforms" in resp
        assert isinstance(resp["platforms"], list)

    def test_list_reports(self, codebuild):
        """ListReports returns report ARNs."""
        resp = codebuild.list_reports()
        assert "reports" in resp
        assert isinstance(resp["reports"], list)

    def test_list_shared_projects(self, codebuild):
        """ListSharedProjects returns shared project ARNs."""
        resp = codebuild.list_shared_projects()
        assert "ResponseMetadata" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_shared_report_groups(self, codebuild):
        """ListSharedReportGroups returns shared report group ARNs."""
        resp = codebuild.list_shared_report_groups()
        assert "ResponseMetadata" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_test_cases(self, codebuild):
        """DescribeTestCases returns test case list for a report."""
        resp = codebuild.describe_test_cases(
            reportArn="arn:aws:codebuild:us-east-1:123456789012:report/fake-report"
        )
        assert "testCases" in resp
        assert isinstance(resp["testCases"], list)

    def test_describe_code_coverages(self, codebuild):
        """DescribeCodeCoverages returns code coverage list for a report."""
        resp = codebuild.describe_code_coverages(
            reportArn="arn:aws:codebuild:us-east-1:123456789012:report/fake-report"
        )
        assert "codeCoverages" in resp
        assert isinstance(resp["codeCoverages"], list)

    def test_get_report_group_trend(self, codebuild):
        """GetReportGroupTrend returns trend stats for a report group."""
        resp = codebuild.get_report_group_trend(
            reportGroupArn="arn:aws:codebuild:us-east-1:123456789012:report-group/fake",
            trendField="TOTAL",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_report(self, codebuild):
        """DeleteReport succeeds for a report ARN."""
        resp = codebuild.delete_report(
            arn="arn:aws:codebuild:us-east-1:123456789012:report/fake-report"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_batch_get_reports_nonexistent(self, codebuild):
        """BatchGetReports with nonexistent ARNs returns empty."""
        resp = codebuild.batch_get_reports(
            reportArns=["arn:aws:codebuild:us-east-1:123456789012:report/nonexistent"]
        )
        assert "reports" in resp

    def test_list_reports_for_report_group(self, codebuild):
        """ListReportsForReportGroup returns reports in a group."""
        name = _unique("rg")
        arn = f"arn:aws:codebuild:us-east-1:123456789012:report-group/{name}"
        try:
            codebuild.create_report_group(
                name=name,
                type="TEST",
                exportConfig={"exportConfigType": "NO_EXPORT"},
            )
            resp = codebuild.list_reports_for_report_group(reportGroupArn=arn)
            assert "reports" in resp
            assert isinstance(resp["reports"], list)
        finally:
            codebuild.delete_report_group(arn=arn)


class TestCodeBuildBatchOperations:
    """Tests for CodeBuild build batch operations."""

    def _create_batch_project(self, codebuild):
        name = _unique("batch-proj")
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
            buildBatchConfig={"serviceRole": "arn:aws:iam::123456789012:role/codebuild-role"},
        )
        return name

    def test_start_build_batch(self, codebuild):
        """StartBuildBatch starts a build batch."""
        name = self._create_batch_project(codebuild)
        try:
            resp = codebuild.start_build_batch(projectName=name)
            batch = resp["buildBatch"]
            assert batch["projectName"] == name
            assert "id" in batch
            assert "arn" in batch
        finally:
            codebuild.delete_project(name=name)

    def test_list_build_batches(self, codebuild):
        """ListBuildBatches returns batch IDs."""
        resp = codebuild.list_build_batches()
        assert "ids" in resp
        assert isinstance(resp["ids"], list)

    def test_list_build_batches_for_project(self, codebuild):
        """ListBuildBatchesForProject returns batch IDs for a project."""
        name = self._create_batch_project(codebuild)
        try:
            resp = codebuild.list_build_batches_for_project(projectName=name)
            assert "ids" in resp
            assert isinstance(resp["ids"], list)
        finally:
            codebuild.delete_project(name=name)

    def test_stop_build_batch(self, codebuild):
        """StopBuildBatch stops a build batch."""
        name = self._create_batch_project(codebuild)
        try:
            b = codebuild.start_build_batch(projectName=name)
            bb_id = b["buildBatch"]["id"]
            resp = codebuild.stop_build_batch(id=bb_id)
            assert "buildBatch" in resp
        finally:
            codebuild.delete_project(name=name)

    def test_delete_build_batch(self, codebuild):
        """DeleteBuildBatch deletes a build batch."""
        name = self._create_batch_project(codebuild)
        try:
            b = codebuild.start_build_batch(projectName=name)
            bb_id = b["buildBatch"]["id"]
            resp = codebuild.delete_build_batch(id=bb_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            codebuild.delete_project(name=name)

    def test_retry_build_batch(self, codebuild):
        """RetryBuildBatch retries a build batch."""
        name = self._create_batch_project(codebuild)
        try:
            b = codebuild.start_build_batch(projectName=name)
            bb_id = b["buildBatch"]["id"]
            resp = codebuild.retry_build_batch(id=bb_id)
            assert "buildBatch" in resp
        finally:
            codebuild.delete_project(name=name)

    def test_batch_get_build_batches(self, codebuild):
        """BatchGetBuildBatches returns batch details."""
        name = self._create_batch_project(codebuild)
        try:
            b = codebuild.start_build_batch(projectName=name)
            bb_id = b["buildBatch"]["id"]
            resp = codebuild.batch_get_build_batches(ids=[bb_id])
            assert len(resp["buildBatches"]) == 1
            assert resp["buildBatches"][0]["id"] == bb_id
        finally:
            codebuild.delete_project(name=name)


class TestCodeBuildSandboxOperations:
    """Tests for CodeBuild sandbox operations."""

    def test_list_sandboxes(self, codebuild):
        """ListSandboxes returns sandbox IDs."""
        resp = codebuild.list_sandboxes()
        assert "ids" in resp
        assert isinstance(resp["ids"], list)

    def test_list_sandboxes_for_project(self, codebuild):
        """ListSandboxesForProject returns sandbox IDs for a project."""
        name = _unique("sbx-proj")
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
            resp = codebuild.list_sandboxes_for_project(projectName=name)
            assert "ids" in resp
            assert isinstance(resp["ids"], list)
        finally:
            codebuild.delete_project(name=name)

    def test_list_command_executions_for_sandbox(self, codebuild):
        """ListCommandExecutionsForSandbox returns command executions."""
        resp = codebuild.list_command_executions_for_sandbox(sandboxId="nonexistent-sandbox-id")
        assert "commandExecutions" in resp
        assert isinstance(resp["commandExecutions"], list)

    def test_batch_get_sandboxes(self, codebuild):
        """BatchGetSandboxes with nonexistent IDs returns empty sandboxes."""
        resp = codebuild.batch_get_sandboxes(ids=["nonexistent-sandbox-id"])
        assert "sandboxes" in resp
        assert isinstance(resp["sandboxes"], list)

    def test_start_sandbox(self, codebuild):
        """StartSandbox returns sandbox details."""
        resp = codebuild.start_sandbox()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "sandbox" in resp

    def test_start_command_execution_nonexistent(self, codebuild):
        """StartCommandExecution on nonexistent sandbox raises ResourceNotFoundException."""
        with pytest.raises(Exception) as exc_info:
            codebuild.start_command_execution(
                sandboxId="nonexistent-sandbox-id",
                command="echo hello",
                type="SHELL",
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_start_sandbox_connection_nonexistent(self, codebuild):
        """StartSandboxConnection on nonexistent sandbox raises ResourceNotFoundException."""
        with pytest.raises(Exception) as exc_info:
            codebuild.start_sandbox_connection(sandboxId="nonexistent-sandbox-id")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_stop_sandbox_nonexistent(self, codebuild):
        """StopSandbox on nonexistent sandbox raises ResourceNotFoundException."""
        with pytest.raises(Exception) as exc_info:
            codebuild.stop_sandbox(id="nonexistent-sandbox-id")
        assert "ResourceNotFoundException" in str(exc_info.value)
