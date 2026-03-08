"""Compatibility tests for AWS AppConfig service."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def appconfig():
    return make_client("appconfig")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestAppConfigApplicationOperations:
    def test_create_application(self, appconfig):
        name = _unique("app")
        resp = appconfig.create_application(Name=name)
        app_id = resp["Id"]
        assert app_id
        assert resp["Name"] == name
        # cleanup
        appconfig.delete_application(ApplicationId=app_id)

    def test_get_application(self, appconfig):
        name = _unique("app")
        create_resp = appconfig.create_application(Name=name)
        app_id = create_resp["Id"]

        resp = appconfig.get_application(ApplicationId=app_id)
        assert resp["Id"] == app_id
        assert resp["Name"] == name
        # cleanup
        appconfig.delete_application(ApplicationId=app_id)

    def test_update_application(self, appconfig):
        name = _unique("app")
        create_resp = appconfig.create_application(Name=name)
        app_id = create_resp["Id"]

        new_name = _unique("app-updated")
        resp = appconfig.update_application(ApplicationId=app_id, Name=new_name)
        assert resp["Name"] == new_name

        get_resp = appconfig.get_application(ApplicationId=app_id)
        assert get_resp["Name"] == new_name
        # cleanup
        appconfig.delete_application(ApplicationId=app_id)

    def test_delete_application(self, appconfig):
        name = _unique("app")
        create_resp = appconfig.create_application(Name=name)
        app_id = create_resp["Id"]

        appconfig.delete_application(ApplicationId=app_id)

        with pytest.raises(ClientError) as exc_info:
            appconfig.get_application(ApplicationId=app_id)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_nonexistent_application(self, appconfig):
        with pytest.raises(ClientError) as exc_info:
            appconfig.get_application(ApplicationId="nonexistent")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestAppConfigConfigProfileOperations:
    def test_create_configuration_profile(self, appconfig):
        app_name = _unique("app")
        app_resp = appconfig.create_application(Name=app_name)
        app_id = app_resp["Id"]

        prof_name = _unique("profile")
        resp = appconfig.create_configuration_profile(
            ApplicationId=app_id,
            Name=prof_name,
            LocationUri="hosted",
        )
        assert resp["Id"]
        assert resp["ApplicationId"] == app_id
        assert resp["Name"] == prof_name
        assert resp["LocationUri"] == "hosted"
        # cleanup
        appconfig.delete_configuration_profile(
            ApplicationId=app_id, ConfigurationProfileId=resp["Id"]
        )
        appconfig.delete_application(ApplicationId=app_id)

    def test_get_configuration_profile(self, appconfig):
        app_resp = appconfig.create_application(Name=_unique("app"))
        app_id = app_resp["Id"]

        prof_name = _unique("profile")
        create_resp = appconfig.create_configuration_profile(
            ApplicationId=app_id,
            Name=prof_name,
            LocationUri="hosted",
        )
        prof_id = create_resp["Id"]

        resp = appconfig.get_configuration_profile(
            ApplicationId=app_id, ConfigurationProfileId=prof_id
        )
        assert resp["Id"] == prof_id
        assert resp["Name"] == prof_name
        assert resp["LocationUri"] == "hosted"
        # cleanup
        appconfig.delete_configuration_profile(ApplicationId=app_id, ConfigurationProfileId=prof_id)
        appconfig.delete_application(ApplicationId=app_id)

    def test_list_configuration_profiles(self, appconfig):
        app_resp = appconfig.create_application(Name=_unique("app"))
        app_id = app_resp["Id"]

        names = [_unique("profile") for _ in range(3)]
        prof_ids = []
        for name in names:
            resp = appconfig.create_configuration_profile(
                ApplicationId=app_id,
                Name=name,
                LocationUri="hosted",
            )
            prof_ids.append(resp["Id"])

        list_resp = appconfig.list_configuration_profiles(ApplicationId=app_id)
        listed_ids = [item["Id"] for item in list_resp["Items"]]
        for pid in prof_ids:
            assert pid in listed_ids
        # cleanup
        for pid in prof_ids:
            appconfig.delete_configuration_profile(ApplicationId=app_id, ConfigurationProfileId=pid)
        appconfig.delete_application(ApplicationId=app_id)

    def test_delete_configuration_profile(self, appconfig):
        app_resp = appconfig.create_application(Name=_unique("app"))
        app_id = app_resp["Id"]

        create_resp = appconfig.create_configuration_profile(
            ApplicationId=app_id,
            Name=_unique("profile"),
            LocationUri="hosted",
        )
        prof_id = create_resp["Id"]

        appconfig.delete_configuration_profile(ApplicationId=app_id, ConfigurationProfileId=prof_id)

        with pytest.raises(ClientError) as exc_info:
            appconfig.get_configuration_profile(
                ApplicationId=app_id, ConfigurationProfileId=prof_id
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
        # cleanup
        appconfig.delete_application(ApplicationId=app_id)


class TestAppConfigHostedConfigVersionOperations:
    """Tests for HostedConfigurationVersion CRUD operations."""

    def _create_app_and_profile(self, appconfig):
        """Helper to create an application and hosted configuration profile."""
        app_resp = appconfig.create_application(Name=_unique("app"))
        app_id = app_resp["Id"]
        prof_resp = appconfig.create_configuration_profile(
            ApplicationId=app_id,
            Name=_unique("profile"),
            LocationUri="hosted",
        )
        prof_id = prof_resp["Id"]
        return app_id, prof_id

    def test_create_hosted_configuration_version(self, appconfig):
        app_id, prof_id = self._create_app_and_profile(appconfig)
        try:
            resp = appconfig.create_hosted_configuration_version(
                ApplicationId=app_id,
                ConfigurationProfileId=prof_id,
                Content=b'{"key": "value"}',
                ContentType="application/json",
            )
            assert resp["VersionNumber"] == 1
            assert resp["ApplicationId"] == app_id
            assert resp["ConfigurationProfileId"] == prof_id
            assert resp["ContentType"] == "application/json"
            assert resp["Content"].read() == b'{"key": "value"}'
        finally:
            appconfig.delete_configuration_profile(
                ApplicationId=app_id, ConfigurationProfileId=prof_id
            )
            appconfig.delete_application(ApplicationId=app_id)

    def test_get_hosted_configuration_version(self, appconfig):
        app_id, prof_id = self._create_app_and_profile(appconfig)
        try:
            create_resp = appconfig.create_hosted_configuration_version(
                ApplicationId=app_id,
                ConfigurationProfileId=prof_id,
                Content=b'{"hello": "world"}',
                ContentType="application/json",
            )
            version = create_resp["VersionNumber"]

            resp = appconfig.get_hosted_configuration_version(
                ApplicationId=app_id,
                ConfigurationProfileId=prof_id,
                VersionNumber=version,
            )
            assert resp["ApplicationId"] == app_id
            assert resp["ConfigurationProfileId"] == prof_id
            assert resp["VersionNumber"] == version
            assert resp["Content"].read() == b'{"hello": "world"}'
            assert resp["ContentType"] == "application/json"
        finally:
            appconfig.delete_configuration_profile(
                ApplicationId=app_id, ConfigurationProfileId=prof_id
            )
            appconfig.delete_application(ApplicationId=app_id)

    def test_delete_hosted_configuration_version(self, appconfig):
        app_id, prof_id = self._create_app_and_profile(appconfig)
        try:
            create_resp = appconfig.create_hosted_configuration_version(
                ApplicationId=app_id,
                ConfigurationProfileId=prof_id,
                Content=b'{"delete": "me"}',
                ContentType="application/json",
            )
            version = create_resp["VersionNumber"]

            appconfig.delete_hosted_configuration_version(
                ApplicationId=app_id,
                ConfigurationProfileId=prof_id,
                VersionNumber=version,
            )

            with pytest.raises(ClientError) as exc_info:
                appconfig.get_hosted_configuration_version(
                    ApplicationId=app_id,
                    ConfigurationProfileId=prof_id,
                    VersionNumber=version,
                )
            assert exc_info.value.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "BadRequestException",
            )
        finally:
            appconfig.delete_configuration_profile(
                ApplicationId=app_id, ConfigurationProfileId=prof_id
            )
            appconfig.delete_application(ApplicationId=app_id)
