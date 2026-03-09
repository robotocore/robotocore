"""Resilience Hub compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def resiliencehub():
    return make_client("resiliencehub")


def _unique_name(prefix: str = "test-app") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestResilienceHubApps:
    def test_list_apps(self, resiliencehub):
        """list_apps returns appSummaries key."""
        response = resiliencehub.list_apps()
        assert "appSummaries" in response

    def test_create_app(self, resiliencehub):
        """create_app returns an app dict with expected fields."""
        name = _unique_name()
        response = resiliencehub.create_app(name=name)
        app = response["app"]
        assert app["name"] == name
        assert "appArn" in app
        assert "status" in app
        assert "creationTime" in app

    def test_create_app_appears_in_list(self, resiliencehub):
        """A created app should appear in list_apps."""
        name = _unique_name()
        create_resp = resiliencehub.create_app(name=name)
        app_arn = create_resp["app"]["appArn"]

        list_resp = resiliencehub.list_apps()
        arns = [s["appArn"] for s in list_resp["appSummaries"]]
        assert app_arn in arns

    def test_describe_app(self, resiliencehub):
        """describe_app returns the app details for a created app."""
        name = _unique_name()
        create_resp = resiliencehub.create_app(name=name)
        app_arn = create_resp["app"]["appArn"]

        describe_resp = resiliencehub.describe_app(appArn=app_arn)
        app = describe_resp["app"]
        assert app["appArn"] == app_arn
        assert app["name"] == name
        assert "status" in app
        assert "creationTime" in app

    def test_create_multiple_apps(self, resiliencehub):
        """Creating multiple apps should all appear in list_apps."""
        names = [_unique_name() for _ in range(3)]
        arns = []
        for name in names:
            resp = resiliencehub.create_app(name=name)
            arns.append(resp["app"]["appArn"])

        list_resp = resiliencehub.list_apps()
        listed_arns = [s["appArn"] for s in list_resp["appSummaries"]]
        for arn in arns:
            assert arn in listed_arns


class TestResilienceHubPolicies:
    def _full_policy(self):
        return {
            "Software": {"rpoInSecs": 3600, "rtoInSecs": 3600},
            "Hardware": {"rpoInSecs": 3600, "rtoInSecs": 3600},
            "AZ": {"rpoInSecs": 3600, "rtoInSecs": 3600},
            "Region": {"rpoInSecs": 3600, "rtoInSecs": 3600},
        }

    def test_list_resiliency_policies(self, resiliencehub):
        """list_resiliency_policies returns resiliencyPolicies key."""
        response = resiliencehub.list_resiliency_policies()
        assert "resiliencyPolicies" in response

    def test_create_resiliency_policy(self, resiliencehub):
        """create_resiliency_policy returns a policy dict."""
        name = _unique_name("test-policy")
        response = resiliencehub.create_resiliency_policy(
            policyName=name,
            tier="NotApplicable",
            policy=self._full_policy(),
        )
        policy = response["policy"]
        assert policy["policyName"] == name
        assert "policyArn" in policy
        assert "tier" in policy

    def test_created_policy_in_list(self, resiliencehub):
        """A created policy should appear in list_resiliency_policies."""
        name = _unique_name("test-policy")
        create_resp = resiliencehub.create_resiliency_policy(
            policyName=name,
            tier="NotApplicable",
            policy=self._full_policy(),
        )
        policy_arn = create_resp["policy"]["policyArn"]

        list_resp = resiliencehub.list_resiliency_policies()
        arns = [p["policyArn"] for p in list_resp["resiliencyPolicies"]]
        assert policy_arn in arns


class TestResilienceHubAppVersions:
    """Tests for AppVersion operations."""

    def _create_app(self, client):
        name = _unique_name()
        resp = client.create_app(name=name)
        return resp["app"]["appArn"]

    def test_publish_app_version(self, resiliencehub):
        """PublishAppVersion returns versionName and appArn."""
        app_arn = self._create_app(resiliencehub)
        resp = resiliencehub.publish_app_version(appArn=app_arn)
        assert "appArn" in resp
        assert "appVersion" in resp

    def test_list_app_versions(self, resiliencehub):
        """ListAppVersions returns appVersions key."""
        app_arn = self._create_app(resiliencehub)
        resp = resiliencehub.list_app_versions(appArn=app_arn)
        assert "appVersions" in resp

    def test_create_app_version_app_component(self, resiliencehub):
        """CreateAppVersionAppComponent returns appComponent."""
        app_arn = self._create_app(resiliencehub)
        resp = resiliencehub.create_app_version_app_component(
            appArn=app_arn,
            name="test-component",
            type="AWS::EC2::Instance",
        )
        assert "appArn" in resp
        assert "appComponent" in resp

    def test_list_app_version_app_components(self, resiliencehub):
        """ListAppVersionAppComponents returns appComponents."""
        app_arn = self._create_app(resiliencehub)
        resiliencehub.create_app_version_app_component(
            appArn=app_arn, name="comp1", type="AWS::EC2::Instance"
        )
        resp = resiliencehub.list_app_version_app_components(appArn=app_arn, appVersion="draft")
        assert "appComponents" in resp

    def test_create_app_version_resource(self, resiliencehub):
        """CreateAppVersionResource returns physicalResource."""
        app_arn = self._create_app(resiliencehub)
        # Create a component first
        resiliencehub.create_app_version_app_component(
            appArn=app_arn,
            name="test-comp",
            type="AWS::EC2::Instance",
        )
        resp = resiliencehub.create_app_version_resource(
            appArn=app_arn,
            appComponents=["test-comp"],
            logicalResourceId={"identifier": "my-resource"},
            physicalResourceId="i-1234567890abcdef0",
            resourceType="AWS::EC2::Instance",
        )
        assert "appArn" in resp
        assert "physicalResource" in resp

    def test_list_app_version_resources(self, resiliencehub):
        """ListAppVersionResources returns physicalResources."""
        app_arn = self._create_app(resiliencehub)
        resiliencehub.create_app_version_app_component(
            appArn=app_arn, name="comp1", type="AWS::EC2::Instance"
        )
        resiliencehub.create_app_version_resource(
            appArn=app_arn,
            appComponents=["comp1"],
            logicalResourceId={"identifier": "res1"},
            physicalResourceId="i-abc123",
            resourceType="AWS::EC2::Instance",
        )
        resp = resiliencehub.list_app_version_resources(appArn=app_arn, appVersion="draft")
        assert "physicalResources" in resp


class TestResilienceHubTags:
    """Tests for Tag operations."""

    def test_tag_resource(self, resiliencehub):
        """TagResource on an app ARN succeeds."""
        name = _unique_name()
        app_arn = resiliencehub.create_app(name=name)["app"]["appArn"]
        resp = resiliencehub.tag_resource(
            resourceArn=app_arn,
            tags={"env": "test"},
        )
        # TagResource returns empty on success (HTTP 200)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_tags_for_resource(self, resiliencehub):
        """ListTagsForResource returns tags dict."""
        name = _unique_name()
        app_arn = resiliencehub.create_app(name=name)["app"]["appArn"]
        resiliencehub.tag_resource(resourceArn=app_arn, tags={"env": "test"})
        resp = resiliencehub.list_tags_for_resource(resourceArn=app_arn)
        assert "tags" in resp
        assert resp["tags"].get("env") == "test"

    def test_untag_resource(self, resiliencehub):
        """UntagResource removes tags from an app."""
        name = _unique_name()
        app_arn = resiliencehub.create_app(name=name)["app"]["appArn"]
        resiliencehub.tag_resource(resourceArn=app_arn, tags={"env": "test", "keep": "yes"})
        resiliencehub.untag_resource(resourceArn=app_arn, tagKeys=["env"])
        resp = resiliencehub.list_tags_for_resource(resourceArn=app_arn)
        assert "env" not in resp.get("tags", {})
        assert resp["tags"].get("keep") == "yes"

    def test_tag_multiple_keys(self, resiliencehub):
        """TagResource with multiple keys stores all of them."""
        name = _unique_name()
        app_arn = resiliencehub.create_app(name=name)["app"]["appArn"]
        resiliencehub.tag_resource(
            resourceArn=app_arn,
            tags={"env": "prod", "team": "platform", "project": "roboto"},
        )
        resp = resiliencehub.list_tags_for_resource(resourceArn=app_arn)
        assert resp["tags"]["env"] == "prod"
        assert resp["tags"]["team"] == "platform"
        assert resp["tags"]["project"] == "roboto"


class TestResilienceHubDescribePolicy:
    """Tests for DescribeResiliencyPolicy."""

    def _full_policy(self):
        return {
            "Software": {"rpoInSecs": 3600, "rtoInSecs": 3600},
            "Hardware": {"rpoInSecs": 3600, "rtoInSecs": 3600},
            "AZ": {"rpoInSecs": 3600, "rtoInSecs": 3600},
            "Region": {"rpoInSecs": 3600, "rtoInSecs": 3600},
        }

    def test_describe_resiliency_policy(self, resiliencehub):
        """DescribeResiliencyPolicy returns policy details."""
        name = _unique_name("test-policy")
        create_resp = resiliencehub.create_resiliency_policy(
            policyName=name,
            tier="NotApplicable",
            policy=self._full_policy(),
        )
        policy_arn = create_resp["policy"]["policyArn"]
        resp = resiliencehub.describe_resiliency_policy(policyArn=policy_arn)
        assert "policy" in resp
        assert resp["policy"]["policyArn"] == policy_arn
        assert resp["policy"]["policyName"] == name


class TestResiliencehubAutoCoverage:
    """Auto-generated coverage tests for resiliencehub."""

    @pytest.fixture
    def client(self):
        return make_client("resiliencehub")

    def test_list_app_assessments(self, client):
        """ListAppAssessments returns a response."""
        resp = client.list_app_assessments()
        assert "assessmentSummaries" in resp
