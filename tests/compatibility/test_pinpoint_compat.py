"""Pinpoint compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def pinpoint():
    return make_client("pinpoint")


class TestPinpointAppOperations:
    def test_create_app(self, pinpoint):
        name = _unique("app")
        resp = pinpoint.create_app(CreateApplicationRequest={"Name": name})
        app_resp = resp["ApplicationResponse"]
        assert app_resp["Name"] == name
        assert "Id" in app_resp
        assert "Arn" in app_resp
        # cleanup
        pinpoint.delete_app(ApplicationId=app_resp["Id"])

    def test_get_app(self, pinpoint):
        name = _unique("app")
        created = pinpoint.create_app(CreateApplicationRequest={"Name": name})
        app_id = created["ApplicationResponse"]["Id"]

        resp = pinpoint.get_app(ApplicationId=app_id)
        assert resp["ApplicationResponse"]["Id"] == app_id
        assert resp["ApplicationResponse"]["Name"] == name
        # cleanup
        pinpoint.delete_app(ApplicationId=app_id)

    def test_get_app_not_found(self, pinpoint):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_app(ApplicationId="nonexistent-app-id")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_apps(self, pinpoint):
        name1 = _unique("app")
        name2 = _unique("app")
        resp1 = pinpoint.create_app(CreateApplicationRequest={"Name": name1})
        resp2 = pinpoint.create_app(CreateApplicationRequest={"Name": name2})
        app_id1 = resp1["ApplicationResponse"]["Id"]
        app_id2 = resp2["ApplicationResponse"]["Id"]

        resp = pinpoint.get_apps()
        items = resp["ApplicationsResponse"]["Item"]
        found_ids = [item["Id"] for item in items]
        assert app_id1 in found_ids
        assert app_id2 in found_ids
        # cleanup
        pinpoint.delete_app(ApplicationId=app_id1)
        pinpoint.delete_app(ApplicationId=app_id2)

    def test_delete_app(self, pinpoint):
        name = _unique("app")
        created = pinpoint.create_app(CreateApplicationRequest={"Name": name})
        app_id = created["ApplicationResponse"]["Id"]

        resp = pinpoint.delete_app(ApplicationId=app_id)
        assert resp["ApplicationResponse"]["Id"] == app_id

        # Verify it no longer exists
        with pytest.raises(ClientError) as exc:
            pinpoint.get_app(ApplicationId=app_id)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_application_settings(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]

        resp = pinpoint.get_application_settings(ApplicationId=app_id)
        settings = resp["ApplicationSettingsResource"]
        assert settings["ApplicationId"] == app_id
        # cleanup
        pinpoint.delete_app(ApplicationId=app_id)

    def test_update_application_settings(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]

        pinpoint.update_application_settings(
            ApplicationId=app_id,
            WriteApplicationSettingsRequest={"Limits": {"Daily": 200}},
        )
        resp = pinpoint.get_application_settings(ApplicationId=app_id)
        assert resp["ApplicationSettingsResource"]["Limits"]["Daily"] == 200
        # cleanup
        pinpoint.delete_app(ApplicationId=app_id)


class TestPinpointTagOperations:
    def test_tag_resource(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_resp = created["ApplicationResponse"]
        app_id = app_resp["Id"]
        arn = app_resp["Arn"]

        pinpoint.tag_resource(
            ResourceArn=arn, TagsModel={"tags": {"env": "test", "team": "backend"}}
        )
        resp = pinpoint.list_tags_for_resource(ResourceArn=arn)
        tags = resp["TagsModel"]["tags"]
        assert tags["env"] == "test"
        assert tags["team"] == "backend"
        # cleanup
        pinpoint.delete_app(ApplicationId=app_id)

    def test_list_tags_for_resource(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_resp = created["ApplicationResponse"]
        app_id = app_resp["Id"]
        arn = app_resp["Arn"]

        # Fresh app should have empty tags
        resp = pinpoint.list_tags_for_resource(ResourceArn=arn)
        assert resp["TagsModel"]["tags"] == {}

        # Add tags and verify
        pinpoint.tag_resource(ResourceArn=arn, TagsModel={"tags": {"key1": "val1"}})
        resp = pinpoint.list_tags_for_resource(ResourceArn=arn)
        assert resp["TagsModel"]["tags"]["key1"] == "val1"
        # cleanup
        pinpoint.delete_app(ApplicationId=app_id)

    def test_untag_resource(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_resp = created["ApplicationResponse"]
        app_id = app_resp["Id"]
        arn = app_resp["Arn"]

        pinpoint.tag_resource(ResourceArn=arn, TagsModel={"tags": {"keep": "yes", "remove": "no"}})
        pinpoint.untag_resource(ResourceArn=arn, TagKeys=["remove"])

        resp = pinpoint.list_tags_for_resource(ResourceArn=arn)
        tags = resp["TagsModel"]["tags"]
        assert "keep" in tags
        assert "remove" not in tags
        # cleanup
        pinpoint.delete_app(ApplicationId=app_id)
