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


class TestResilienceHubAppDetails:
    """Deeper tests for app creation with various parameters."""

    def test_create_app_with_description(self, resiliencehub):
        """CreateApp with description stores and returns it."""
        name = _unique_name()
        resp = resiliencehub.create_app(name=name, description="My test application")
        app = resp["app"]
        assert app["name"] == name
        assert app["description"] == "My test application"

    def test_create_app_with_tags(self, resiliencehub):
        """CreateApp with tags makes them visible via ListTagsForResource."""
        name = _unique_name()
        resp = resiliencehub.create_app(name=name, tags={"env": "test", "team": "platform"})
        app_arn = resp["app"]["appArn"]
        tags_resp = resiliencehub.list_tags_for_resource(resourceArn=app_arn)
        assert tags_resp["tags"]["env"] == "test"
        assert tags_resp["tags"]["team"] == "platform"

    def test_create_app_with_assessment_schedule(self, resiliencehub):
        """CreateApp with assessmentSchedule returns the schedule."""
        name = _unique_name()
        resp = resiliencehub.create_app(name=name, assessmentSchedule="Daily")
        assert resp["app"]["assessmentSchedule"] == "Daily"

    def test_create_app_with_policy_arn(self, resiliencehub):
        """CreateApp with policyArn links the policy to the app."""
        policy_resp = resiliencehub.create_resiliency_policy(
            policyName=_unique_name("policy"),
            tier="NotApplicable",
            policy={
                "Software": {"rpoInSecs": 3600, "rtoInSecs": 3600},
                "Hardware": {"rpoInSecs": 3600, "rtoInSecs": 3600},
                "AZ": {"rpoInSecs": 3600, "rtoInSecs": 3600},
                "Region": {"rpoInSecs": 3600, "rtoInSecs": 3600},
            },
        )
        policy_arn = policy_resp["policy"]["policyArn"]
        app_resp = resiliencehub.create_app(name=_unique_name(), policyArn=policy_arn)
        assert app_resp["app"]["policyArn"] == policy_arn

    def test_describe_app_nonexistent_raises(self, resiliencehub):
        """DescribeApp with a fake ARN raises ResourceNotFoundException."""
        with pytest.raises(resiliencehub.exceptions.ClientError) as exc_info:
            resiliencehub.describe_app(
                appArn="arn:aws:resiliencehub:us-east-1:123456789012:app/nonexistent"
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_app_status_is_active(self, resiliencehub):
        """Newly created apps have Active status."""
        resp = resiliencehub.create_app(name=_unique_name())
        assert resp["app"]["status"] == "Active"

    def test_list_apps_filter_by_name(self, resiliencehub):
        """ListApps with name filter returns only matching apps."""
        target_name = _unique_name("filter-target")
        resiliencehub.create_app(name=target_name)
        resiliencehub.create_app(name=_unique_name("filter-other"))

        resp = resiliencehub.list_apps(name=target_name)
        assert len(resp["appSummaries"]) >= 1
        names = [s["name"] for s in resp["appSummaries"]]
        assert target_name in names


class TestResilienceHubPolicyDetails:
    """Deeper tests for resiliency policy operations."""

    def _full_policy(self):
        return {
            "Software": {"rpoInSecs": 3600, "rtoInSecs": 3600},
            "Hardware": {"rpoInSecs": 3600, "rtoInSecs": 3600},
            "AZ": {"rpoInSecs": 3600, "rtoInSecs": 3600},
            "Region": {"rpoInSecs": 3600, "rtoInSecs": 3600},
        }

    def test_create_policy_with_description(self, resiliencehub):
        """CreateResiliencyPolicy with description stores it."""
        name = _unique_name("policy")
        resp = resiliencehub.create_resiliency_policy(
            policyName=name,
            tier="NotApplicable",
            policyDescription="A test policy description",
            policy=self._full_policy(),
        )
        assert resp["policy"]["policyDescription"] == "A test policy description"

    def test_create_policy_mission_critical_tier(self, resiliencehub):
        """CreateResiliencyPolicy with MissionCritical tier."""
        resp = resiliencehub.create_resiliency_policy(
            policyName=_unique_name("policy"),
            tier="MissionCritical",
            policy=self._full_policy(),
        )
        assert resp["policy"]["tier"] == "MissionCritical"

    def test_create_policy_critical_tier(self, resiliencehub):
        """CreateResiliencyPolicy with Critical tier."""
        resp = resiliencehub.create_resiliency_policy(
            policyName=_unique_name("policy"),
            tier="Critical",
            policy=self._full_policy(),
        )
        assert resp["policy"]["tier"] == "Critical"

    def test_create_policy_important_tier(self, resiliencehub):
        """CreateResiliencyPolicy with Important tier."""
        resp = resiliencehub.create_resiliency_policy(
            policyName=_unique_name("policy"),
            tier="Important",
            policy=self._full_policy(),
        )
        assert resp["policy"]["tier"] == "Important"

    def test_create_policy_non_critical_tier(self, resiliencehub):
        """CreateResiliencyPolicy with NonCritical tier."""
        resp = resiliencehub.create_resiliency_policy(
            policyName=_unique_name("policy"),
            tier="NonCritical",
            policy=self._full_policy(),
        )
        assert resp["policy"]["tier"] == "NonCritical"

    def test_create_policy_core_services_tier(self, resiliencehub):
        """CreateResiliencyPolicy with CoreServices tier."""
        resp = resiliencehub.create_resiliency_policy(
            policyName=_unique_name("policy"),
            tier="CoreServices",
            policy=self._full_policy(),
        )
        assert resp["policy"]["tier"] == "CoreServices"

    def test_create_policy_with_custom_rpo_rto(self, resiliencehub):
        """CreateResiliencyPolicy with custom RPO/RTO values are stored correctly."""
        resp = resiliencehub.create_resiliency_policy(
            policyName=_unique_name("policy"),
            tier="NotApplicable",
            policy={
                "Software": {"rpoInSecs": 300, "rtoInSecs": 600},
                "Hardware": {"rpoInSecs": 900, "rtoInSecs": 1800},
                "AZ": {"rpoInSecs": 7200, "rtoInSecs": 14400},
                "Region": {"rpoInSecs": 86400, "rtoInSecs": 172800},
            },
        )
        policy_data = resp["policy"]["policy"]
        assert policy_data["Software"]["rpoInSecs"] == 300
        assert policy_data["Software"]["rtoInSecs"] == 600
        assert policy_data["Hardware"]["rpoInSecs"] == 900
        assert policy_data["Region"]["rtoInSecs"] == 172800

    def test_create_policy_with_tags(self, resiliencehub):
        """CreateResiliencyPolicy with tags stores them."""
        name = _unique_name("policy")
        resp = resiliencehub.create_resiliency_policy(
            policyName=name,
            tier="NotApplicable",
            policy=self._full_policy(),
            tags={"env": "staging"},
        )
        policy_arn = resp["policy"]["policyArn"]
        tags_resp = resiliencehub.list_tags_for_resource(resourceArn=policy_arn)
        assert tags_resp["tags"]["env"] == "staging"

    def test_describe_resiliency_policy_nonexistent_raises(self, resiliencehub):
        """DescribeResiliencyPolicy with fake ARN raises ResourceNotFoundException."""
        with pytest.raises(resiliencehub.exceptions.ClientError) as exc_info:
            resiliencehub.describe_resiliency_policy(
                policyArn="arn:aws:resiliencehub:us-east-1:123456789012:resiliency-policy/fake"
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_multiple_policies_all_in_list(self, resiliencehub):
        """Multiple created policies all appear in list_resiliency_policies."""
        arns = []
        for _ in range(3):
            resp = resiliencehub.create_resiliency_policy(
                policyName=_unique_name("policy"),
                tier="NotApplicable",
                policy=self._full_policy(),
            )
            arns.append(resp["policy"]["policyArn"])
        list_resp = resiliencehub.list_resiliency_policies()
        listed_arns = [p["policyArn"] for p in list_resp["resiliencyPolicies"]]
        for arn in arns:
            assert arn in listed_arns


class TestResilienceHubVersionsDeep:
    """Deeper tests for app version operations."""

    def _create_app(self, client):
        return client.create_app(name=_unique_name())["app"]["appArn"]

    def test_publish_creates_new_version(self, resiliencehub):
        """Publishing creates a new version visible in list_app_versions."""
        app_arn = self._create_app(resiliencehub)
        resiliencehub.publish_app_version(appArn=app_arn)
        resp = resiliencehub.list_app_versions(appArn=app_arn)
        assert len(resp["appVersions"]) >= 1

    def test_publish_app_version_returns_app_arn(self, resiliencehub):
        """PublishAppVersion returns the correct appArn."""
        app_arn = self._create_app(resiliencehub)
        resp = resiliencehub.publish_app_version(appArn=app_arn)
        assert resp["appArn"] == app_arn

    def test_component_type_preserved(self, resiliencehub):
        """CreateAppVersionAppComponent preserves the type."""
        app_arn = self._create_app(resiliencehub)
        resp = resiliencehub.create_app_version_app_component(
            appArn=app_arn, name="my-comp", type="AWS::EC2::Instance"
        )
        assert resp["appComponent"]["type"] == "AWS::EC2::Instance"

    def test_component_name_preserved(self, resiliencehub):
        """CreateAppVersionAppComponent preserves the name."""
        app_arn = self._create_app(resiliencehub)
        resp = resiliencehub.create_app_version_app_component(
            appArn=app_arn, name="my-named-comp", type="AWS::EC2::Instance"
        )
        assert resp["appComponent"]["name"] == "my-named-comp"

    def test_multiple_components_counted(self, resiliencehub):
        """Creating multiple components shows them all in the list."""
        app_arn = self._create_app(resiliencehub)
        for i in range(3):
            resiliencehub.create_app_version_app_component(
                appArn=app_arn, name=f"comp-{i}", type="AWS::EC2::Instance"
            )
        resp = resiliencehub.list_app_version_app_components(appArn=app_arn, appVersion="draft")
        assert len(resp["appComponents"]) == 3

    def test_resource_links_to_component(self, resiliencehub):
        """CreateAppVersionResource links resource to the specified component."""
        app_arn = self._create_app(resiliencehub)
        resiliencehub.create_app_version_app_component(
            appArn=app_arn, name="linked-comp", type="AWS::EC2::Instance"
        )
        resp = resiliencehub.create_app_version_resource(
            appArn=app_arn,
            appComponents=["linked-comp"],
            logicalResourceId={"identifier": "linked-res"},
            physicalResourceId="i-linked123",
            resourceType="AWS::EC2::Instance",
        )
        resource = resp["physicalResource"]
        assert resource["resourceType"] == "AWS::EC2::Instance"
        assert "logicalResourceId" in resource

    def test_multiple_resources_counted(self, resiliencehub):
        """Creating multiple resources shows them all in the list."""
        app_arn = self._create_app(resiliencehub)
        resiliencehub.create_app_version_app_component(
            appArn=app_arn, name="multi-comp", type="AWS::EC2::Instance"
        )
        for i in range(3):
            resiliencehub.create_app_version_resource(
                appArn=app_arn,
                appComponents=["multi-comp"],
                logicalResourceId={"identifier": f"res-{i}"},
                physicalResourceId=f"i-multi{i:03d}",
                resourceType="AWS::EC2::Instance",
            )
        resp = resiliencehub.list_app_version_resources(appArn=app_arn, appVersion="draft")
        assert len(resp["physicalResources"]) == 3

    def test_component_version_is_draft(self, resiliencehub):
        """CreateAppVersionAppComponent returns appVersion as draft."""
        app_arn = self._create_app(resiliencehub)
        resp = resiliencehub.create_app_version_app_component(
            appArn=app_arn, name="draft-comp", type="AWS::EC2::Instance"
        )
        assert resp["appVersion"] == "draft"

    def test_resource_version_is_draft(self, resiliencehub):
        """CreateAppVersionResource returns appVersion as draft."""
        app_arn = self._create_app(resiliencehub)
        resiliencehub.create_app_version_app_component(
            appArn=app_arn, name="draft-res-comp", type="AWS::EC2::Instance"
        )
        resp = resiliencehub.create_app_version_resource(
            appArn=app_arn,
            appComponents=["draft-res-comp"],
            logicalResourceId={"identifier": "draft-res"},
            physicalResourceId="i-draft123",
            resourceType="AWS::EC2::Instance",
        )
        assert resp["appVersion"] == "draft"


class TestResilienceHubTagsDeep:
    """Deeper tag operation tests."""

    def test_tag_policy_resource(self, resiliencehub):
        """TagResource works on policy ARNs."""
        resp = resiliencehub.create_resiliency_policy(
            policyName=_unique_name("policy"),
            tier="NotApplicable",
            policy={
                "Software": {"rpoInSecs": 3600, "rtoInSecs": 3600},
                "Hardware": {"rpoInSecs": 3600, "rtoInSecs": 3600},
                "AZ": {"rpoInSecs": 3600, "rtoInSecs": 3600},
                "Region": {"rpoInSecs": 3600, "rtoInSecs": 3600},
            },
        )
        policy_arn = resp["policy"]["policyArn"]
        resiliencehub.tag_resource(resourceArn=policy_arn, tags={"cost": "low"})
        tags_resp = resiliencehub.list_tags_for_resource(resourceArn=policy_arn)
        assert tags_resp["tags"]["cost"] == "low"

    def test_tag_overwrite(self, resiliencehub):
        """TagResource overwrites existing tag value for same key."""
        app_arn = resiliencehub.create_app(name=_unique_name())["app"]["appArn"]
        resiliencehub.tag_resource(resourceArn=app_arn, tags={"env": "dev"})
        resiliencehub.tag_resource(resourceArn=app_arn, tags={"env": "prod"})
        tags_resp = resiliencehub.list_tags_for_resource(resourceArn=app_arn)
        assert tags_resp["tags"]["env"] == "prod"

    def test_untag_multiple_keys(self, resiliencehub):
        """UntagResource removes multiple keys at once."""
        app_arn = resiliencehub.create_app(name=_unique_name())["app"]["appArn"]
        resiliencehub.tag_resource(
            resourceArn=app_arn,
            tags={"a": "1", "b": "2", "c": "3"},
        )
        resiliencehub.untag_resource(resourceArn=app_arn, tagKeys=["a", "b"])
        tags_resp = resiliencehub.list_tags_for_resource(resourceArn=app_arn)
        assert "a" not in tags_resp.get("tags", {})
        assert "b" not in tags_resp.get("tags", {})
        assert tags_resp["tags"].get("c") == "3"

    def test_list_tags_empty_initially(self, resiliencehub):
        """ListTagsForResource on an untagged app returns empty or missing tags."""
        app_arn = resiliencehub.create_app(name=_unique_name())["app"]["appArn"]
        resp = resiliencehub.list_tags_for_resource(resourceArn=app_arn)
        assert "tags" in resp
        # Tags may be empty dict or contain no user-defined keys


class TestResiliencehubAutoCoverage:
    """Auto-generated coverage tests for resiliencehub."""

    @pytest.fixture
    def client(self):
        return make_client("resiliencehub")

    def test_list_app_assessments(self, client):
        """ListAppAssessments returns a response."""
        resp = client.list_app_assessments()
        assert "assessmentSummaries" in resp
