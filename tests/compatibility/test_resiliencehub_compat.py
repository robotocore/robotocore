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


class TestResiliencehubAutoCoverage:
    """Auto-generated coverage tests for resiliencehub."""

    @pytest.fixture
    def client(self):
        return make_client("resiliencehub")

    def test_list_app_assessments(self, client):
        """ListAppAssessments returns a response."""
        resp = client.list_app_assessments()
        assert "assessmentSummaries" in resp
