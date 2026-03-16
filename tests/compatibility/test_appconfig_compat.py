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


class TestAppConfigTagOperations:
    """Tests for ListTagsForResource, TagResource, UntagResource on AppConfig."""

    def test_list_tags_for_resource(self, appconfig):
        """ListTagsForResource returns tags on an application."""
        name = _unique("app")
        resp = appconfig.create_application(Name=name)
        app_id = resp["Id"]
        try:
            app_arn = f"arn:aws:appconfig:us-east-1:123456789012:application/{app_id}"
            tags_resp = appconfig.list_tags_for_resource(ResourceArn=app_arn)
            assert "Tags" in tags_resp
            assert isinstance(tags_resp["Tags"], dict)
        finally:
            appconfig.delete_application(ApplicationId=app_id)

    def test_tag_resource(self, appconfig):
        """TagResource adds tags to an application."""
        name = _unique("app")
        resp = appconfig.create_application(Name=name)
        app_id = resp["Id"]
        try:
            app_arn = f"arn:aws:appconfig:us-east-1:123456789012:application/{app_id}"
            appconfig.tag_resource(
                ResourceArn=app_arn,
                Tags={"env": "test", "project": "roboto"},
            )
            tags_resp = appconfig.list_tags_for_resource(ResourceArn=app_arn)
            assert tags_resp["Tags"]["env"] == "test"
            assert tags_resp["Tags"]["project"] == "roboto"
        finally:
            appconfig.delete_application(ApplicationId=app_id)

    def test_untag_resource(self, appconfig):
        """UntagResource removes tags from an application."""
        name = _unique("app")
        resp = appconfig.create_application(Name=name)
        app_id = resp["Id"]
        try:
            app_arn = f"arn:aws:appconfig:us-east-1:123456789012:application/{app_id}"
            appconfig.tag_resource(
                ResourceArn=app_arn,
                Tags={"env": "test", "keep": "yes"},
            )
            appconfig.untag_resource(ResourceArn=app_arn, TagKeys=["env"])
            tags_resp = appconfig.list_tags_for_resource(ResourceArn=app_arn)
            assert "env" not in tags_resp["Tags"]
            assert tags_resp["Tags"]["keep"] == "yes"
        finally:
            appconfig.delete_application(ApplicationId=app_id)


class TestAppConfigUpdateConfigProfile:
    """Tests for UpdateConfigurationProfile."""

    def test_update_configuration_profile(self, appconfig):
        """UpdateConfigurationProfile changes the profile name."""
        app_resp = appconfig.create_application(Name=_unique("app"))
        app_id = app_resp["Id"]
        prof_resp = appconfig.create_configuration_profile(
            ApplicationId=app_id,
            Name=_unique("profile"),
            LocationUri="hosted",
        )
        prof_id = prof_resp["Id"]
        try:
            new_name = _unique("updated-profile")
            resp = appconfig.update_configuration_profile(
                ApplicationId=app_id,
                ConfigurationProfileId=prof_id,
                Name=new_name,
            )
            assert resp["Name"] == new_name
            # Verify via get
            get_resp = appconfig.get_configuration_profile(
                ApplicationId=app_id, ConfigurationProfileId=prof_id
            )
            assert get_resp["Name"] == new_name
        finally:
            appconfig.delete_configuration_profile(
                ApplicationId=app_id, ConfigurationProfileId=prof_id
            )
            appconfig.delete_application(ApplicationId=app_id)

    def test_update_configuration_profile_description(self, appconfig):
        """UpdateConfigurationProfile can set a description."""
        app_resp = appconfig.create_application(Name=_unique("app"))
        app_id = app_resp["Id"]
        prof_resp = appconfig.create_configuration_profile(
            ApplicationId=app_id,
            Name=_unique("profile"),
            LocationUri="hosted",
        )
        prof_id = prof_resp["Id"]
        try:
            resp = appconfig.update_configuration_profile(
                ApplicationId=app_id,
                ConfigurationProfileId=prof_id,
                Description="Updated description",
            )
            assert resp["Description"] == "Updated description"
        finally:
            appconfig.delete_configuration_profile(
                ApplicationId=app_id, ConfigurationProfileId=prof_id
            )
            appconfig.delete_application(ApplicationId=app_id)


class TestAppConfigMultipleVersions:
    """Tests for multiple hosted configuration versions."""

    def test_multiple_versions_increment(self, appconfig):
        """Each new hosted config version increments the version number."""
        app_resp = appconfig.create_application(Name=_unique("app"))
        app_id = app_resp["Id"]
        prof_resp = appconfig.create_configuration_profile(
            ApplicationId=app_id,
            Name=_unique("profile"),
            LocationUri="hosted",
        )
        prof_id = prof_resp["Id"]
        try:
            v1 = appconfig.create_hosted_configuration_version(
                ApplicationId=app_id,
                ConfigurationProfileId=prof_id,
                Content=b'{"version": 1}',
                ContentType="application/json",
            )
            v2 = appconfig.create_hosted_configuration_version(
                ApplicationId=app_id,
                ConfigurationProfileId=prof_id,
                Content=b'{"version": 2}',
                ContentType="application/json",
            )
            assert v1["VersionNumber"] == 1
            assert v2["VersionNumber"] == 2
        finally:
            appconfig.delete_configuration_profile(
                ApplicationId=app_id, ConfigurationProfileId=prof_id
            )
            appconfig.delete_application(ApplicationId=app_id)


class TestAppConfigCreateApplicationWithTags:
    """Tests for creating applications with initial tags."""

    def test_create_application_with_tags(self, appconfig):
        """CreateApplication with Tags stores them."""
        name = _unique("app")
        resp = appconfig.create_application(
            Name=name,
            Tags={"env": "dev", "team": "platform"},
        )
        app_id = resp["Id"]
        try:
            app_arn = f"arn:aws:appconfig:us-east-1:123456789012:application/{app_id}"
            tags_resp = appconfig.list_tags_for_resource(ResourceArn=app_arn)
            assert tags_resp["Tags"].get("env") == "dev"
            assert tags_resp["Tags"].get("team") == "platform"
        finally:
            appconfig.delete_application(ApplicationId=app_id)


class TestAppConfigListApplications:
    """Tests for ListApplications."""

    def test_list_applications(self, appconfig):
        """ListApplications returns created applications."""
        names = [_unique("app") for _ in range(3)]
        app_ids = []
        for name in names:
            resp = appconfig.create_application(Name=name)
            app_ids.append(resp["Id"])
        try:
            list_resp = appconfig.list_applications()
            listed_ids = [item["Id"] for item in list_resp["Items"]]
            for aid in app_ids:
                assert aid in listed_ids
        finally:
            for aid in app_ids:
                appconfig.delete_application(ApplicationId=aid)


class TestAppConfigEnvironmentOperations:
    """Tests for Environment CRUD operations."""

    def _create_app(self, appconfig):
        resp = appconfig.create_application(Name=_unique("app"))
        return resp["Id"]

    def test_create_environment(self, appconfig):
        app_id = self._create_app(appconfig)
        try:
            env_name = _unique("env")
            resp = appconfig.create_environment(ApplicationId=app_id, Name=env_name)
            assert resp["Id"]
            assert resp["Name"] == env_name
            assert resp["ApplicationId"] == app_id
            appconfig.delete_environment(ApplicationId=app_id, EnvironmentId=resp["Id"])
        finally:
            appconfig.delete_application(ApplicationId=app_id)

    def test_get_environment(self, appconfig):
        app_id = self._create_app(appconfig)
        try:
            env_name = _unique("env")
            create_resp = appconfig.create_environment(ApplicationId=app_id, Name=env_name)
            env_id = create_resp["Id"]

            resp = appconfig.get_environment(ApplicationId=app_id, EnvironmentId=env_id)
            assert resp["Id"] == env_id
            assert resp["Name"] == env_name
            appconfig.delete_environment(ApplicationId=app_id, EnvironmentId=env_id)
        finally:
            appconfig.delete_application(ApplicationId=app_id)

    def test_list_environments(self, appconfig):
        app_id = self._create_app(appconfig)
        try:
            env_ids = []
            for _ in range(2):
                resp = appconfig.create_environment(ApplicationId=app_id, Name=_unique("env"))
                env_ids.append(resp["Id"])

            list_resp = appconfig.list_environments(ApplicationId=app_id)
            listed_ids = [item["Id"] for item in list_resp["Items"]]
            for eid in env_ids:
                assert eid in listed_ids

            for eid in env_ids:
                appconfig.delete_environment(ApplicationId=app_id, EnvironmentId=eid)
        finally:
            appconfig.delete_application(ApplicationId=app_id)

    def test_update_environment(self, appconfig):
        app_id = self._create_app(appconfig)
        try:
            create_resp = appconfig.create_environment(ApplicationId=app_id, Name=_unique("env"))
            env_id = create_resp["Id"]

            new_name = _unique("env-updated")
            resp = appconfig.update_environment(
                ApplicationId=app_id, EnvironmentId=env_id, Name=new_name
            )
            assert resp["Name"] == new_name

            appconfig.delete_environment(ApplicationId=app_id, EnvironmentId=env_id)
        finally:
            appconfig.delete_application(ApplicationId=app_id)

    def test_delete_environment(self, appconfig):
        app_id = self._create_app(appconfig)
        try:
            create_resp = appconfig.create_environment(ApplicationId=app_id, Name=_unique("env"))
            env_id = create_resp["Id"]

            appconfig.delete_environment(ApplicationId=app_id, EnvironmentId=env_id)

            with pytest.raises(ClientError) as exc_info:
                appconfig.get_environment(ApplicationId=app_id, EnvironmentId=env_id)
            assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            appconfig.delete_application(ApplicationId=app_id)


class TestAppConfigDeploymentStrategyOperations:
    """Tests for DeploymentStrategy operations."""

    def test_create_and_get_deployment_strategy(self, appconfig):
        name = _unique("strategy")
        create_resp = appconfig.create_deployment_strategy(
            Name=name,
            DeploymentDurationInMinutes=10,
            GrowthFactor=25.0,
            ReplicateTo="NONE",
        )
        strategy_id = create_resp["Id"]
        assert strategy_id
        assert create_resp["Name"] == name
        assert create_resp["DeploymentDurationInMinutes"] == 10

        try:
            get_resp = appconfig.get_deployment_strategy(DeploymentStrategyId=strategy_id)
            assert get_resp["Id"] == strategy_id
            assert get_resp["Name"] == name
        finally:
            appconfig.delete_deployment_strategy(DeploymentStrategyId=strategy_id)

    def test_list_deployment_strategies(self, appconfig):
        name = _unique("strategy")
        create_resp = appconfig.create_deployment_strategy(
            Name=name,
            DeploymentDurationInMinutes=5,
            GrowthFactor=50.0,
            ReplicateTo="NONE",
        )
        strategy_id = create_resp["Id"]
        try:
            list_resp = appconfig.list_deployment_strategies()
            listed_ids = [item["Id"] for item in list_resp["Items"]]
            assert strategy_id in listed_ids
        finally:
            appconfig.delete_deployment_strategy(DeploymentStrategyId=strategy_id)

    def test_update_deployment_strategy(self, appconfig):
        name = _unique("strategy")
        create_resp = appconfig.create_deployment_strategy(
            Name=name,
            DeploymentDurationInMinutes=10,
            GrowthFactor=25.0,
            ReplicateTo="NONE",
        )
        strategy_id = create_resp["Id"]
        try:
            resp = appconfig.update_deployment_strategy(
                DeploymentStrategyId=strategy_id,
                DeploymentDurationInMinutes=20,
            )
            assert resp["DeploymentDurationInMinutes"] == 20
        finally:
            appconfig.delete_deployment_strategy(DeploymentStrategyId=strategy_id)

    def test_delete_deployment_strategy(self, appconfig):
        create_resp = appconfig.create_deployment_strategy(
            Name=_unique("strategy"),
            DeploymentDurationInMinutes=5,
            GrowthFactor=50.0,
            ReplicateTo="NONE",
        )
        strategy_id = create_resp["Id"]
        appconfig.delete_deployment_strategy(DeploymentStrategyId=strategy_id)
        # Verify it's gone - should raise
        with pytest.raises(ClientError) as exc_info:
            appconfig.get_deployment_strategy(DeploymentStrategyId=strategy_id)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestAppConfigExtensionOperations:
    """Tests for Extension CRUD operations."""

    def test_create_extension(self, appconfig):
        name = _unique("ext")
        resp = appconfig.create_extension(
            Name=name,
            Actions={
                "PRE_CREATE_HOSTED_CONFIGURATION_VERSION": [
                    {
                        "Name": "MyAction",
                        "Uri": "arn:aws:lambda:us-east-1:123456789012:function:my-func",
                    }
                ]
            },
        )
        ext_id = resp["Id"]
        assert ext_id
        assert resp["Name"] == name
        try:
            appconfig.delete_extension(ExtensionIdentifier=ext_id)
        except Exception:
            pass  # best-effort cleanup

    def test_get_extension(self, appconfig):
        name = _unique("ext")
        create_resp = appconfig.create_extension(
            Name=name,
            Actions={
                "PRE_CREATE_HOSTED_CONFIGURATION_VERSION": [
                    {
                        "Name": "MyAction",
                        "Uri": "arn:aws:lambda:us-east-1:123456789012:function:my-func",
                    }
                ]
            },
        )
        ext_id = create_resp["Id"]
        try:
            resp = appconfig.get_extension(ExtensionIdentifier=ext_id)
            assert resp["Id"] == ext_id
            assert resp["Name"] == name
        finally:
            appconfig.delete_extension(ExtensionIdentifier=ext_id)

    def test_list_extensions(self, appconfig):
        name = _unique("ext")
        create_resp = appconfig.create_extension(
            Name=name,
            Actions={
                "PRE_CREATE_HOSTED_CONFIGURATION_VERSION": [
                    {
                        "Name": "MyAction",
                        "Uri": "arn:aws:lambda:us-east-1:123456789012:function:my-func",
                    }
                ]
            },
        )
        ext_id = create_resp["Id"]
        try:
            list_resp = appconfig.list_extensions()
            listed_ids = [item["Id"] for item in list_resp["Items"]]
            assert ext_id in listed_ids
        finally:
            appconfig.delete_extension(ExtensionIdentifier=ext_id)

    def test_update_extension(self, appconfig):
        name = _unique("ext")
        create_resp = appconfig.create_extension(
            Name=name,
            Actions={
                "PRE_CREATE_HOSTED_CONFIGURATION_VERSION": [
                    {
                        "Name": "MyAction",
                        "Uri": "arn:aws:lambda:us-east-1:123456789012:function:my-func",
                    }
                ]
            },
        )
        ext_id = create_resp["Id"]
        version = create_resp["VersionNumber"]
        try:
            resp = appconfig.update_extension(
                ExtensionIdentifier=ext_id,
                VersionNumber=version,
                Description="Updated description",
            )
            assert resp["Description"] == "Updated description"
        finally:
            appconfig.delete_extension(ExtensionIdentifier=ext_id)

    def test_delete_extension(self, appconfig):
        create_resp = appconfig.create_extension(
            Name=_unique("ext"),
            Actions={
                "PRE_CREATE_HOSTED_CONFIGURATION_VERSION": [
                    {
                        "Name": "MyAction",
                        "Uri": "arn:aws:lambda:us-east-1:123456789012:function:my-func",
                    }
                ]
            },
        )
        ext_id = create_resp["Id"]
        appconfig.delete_extension(ExtensionIdentifier=ext_id)
        with pytest.raises(ClientError) as exc_info:
            appconfig.get_extension(ExtensionIdentifier=ext_id)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestAppConfigExtensionAssociationOperations:
    """Tests for ExtensionAssociation operations."""

    def _create_app_and_extension(self, appconfig):
        app_resp = appconfig.create_application(Name=_unique("app"))
        app_id = app_resp["Id"]
        ext_resp = appconfig.create_extension(
            Name=_unique("ext"),
            Actions={
                "PRE_CREATE_HOSTED_CONFIGURATION_VERSION": [
                    {
                        "Name": "MyAction",
                        "Uri": "arn:aws:lambda:us-east-1:123456789012:function:my-func",
                    }
                ]
            },
        )
        ext_id = ext_resp["Id"]
        return app_id, ext_id

    def test_create_extension_association(self, appconfig):
        app_id, ext_id = self._create_app_and_extension(appconfig)
        try:
            app_arn = f"arn:aws:appconfig:us-east-1:123456789012:application/{app_id}"
            resp = appconfig.create_extension_association(
                ExtensionIdentifier=ext_id,
                ResourceIdentifier=app_arn,
            )
            assoc_id = resp["Id"]
            assert assoc_id
            assert resp["ExtensionArn"]
            appconfig.delete_extension_association(ExtensionAssociationId=assoc_id)
        finally:
            appconfig.delete_extension(ExtensionIdentifier=ext_id)
            appconfig.delete_application(ApplicationId=app_id)

    def test_get_extension_association(self, appconfig):
        app_id, ext_id = self._create_app_and_extension(appconfig)
        try:
            app_arn = f"arn:aws:appconfig:us-east-1:123456789012:application/{app_id}"
            create_resp = appconfig.create_extension_association(
                ExtensionIdentifier=ext_id,
                ResourceIdentifier=app_arn,
            )
            assoc_id = create_resp["Id"]

            resp = appconfig.get_extension_association(ExtensionAssociationId=assoc_id)
            assert resp["Id"] == assoc_id
            assert resp["ExtensionArn"]

            appconfig.delete_extension_association(ExtensionAssociationId=assoc_id)
        finally:
            appconfig.delete_extension(ExtensionIdentifier=ext_id)
            appconfig.delete_application(ApplicationId=app_id)

    def test_list_extension_associations(self, appconfig):
        app_id, ext_id = self._create_app_and_extension(appconfig)
        try:
            app_arn = f"arn:aws:appconfig:us-east-1:123456789012:application/{app_id}"
            create_resp = appconfig.create_extension_association(
                ExtensionIdentifier=ext_id,
                ResourceIdentifier=app_arn,
            )
            assoc_id = create_resp["Id"]

            list_resp = appconfig.list_extension_associations(
                ResourceIdentifier=app_arn,
            )
            listed_ids = [item["Id"] for item in list_resp["Items"]]
            assert assoc_id in listed_ids

            appconfig.delete_extension_association(ExtensionAssociationId=assoc_id)
        finally:
            appconfig.delete_extension(ExtensionIdentifier=ext_id)
            appconfig.delete_application(ApplicationId=app_id)

    def test_delete_extension_association(self, appconfig):
        app_id, ext_id = self._create_app_and_extension(appconfig)
        try:
            app_arn = f"arn:aws:appconfig:us-east-1:123456789012:application/{app_id}"
            create_resp = appconfig.create_extension_association(
                ExtensionIdentifier=ext_id,
                ResourceIdentifier=app_arn,
            )
            assoc_id = create_resp["Id"]

            appconfig.delete_extension_association(ExtensionAssociationId=assoc_id)
            with pytest.raises(ClientError) as exc_info:
                appconfig.get_extension_association(ExtensionAssociationId=assoc_id)
            assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            appconfig.delete_extension(ExtensionIdentifier=ext_id)
            appconfig.delete_application(ApplicationId=app_id)


class TestAppConfigHostedConfigVersionList:
    """Tests for ListHostedConfigurationVersions."""

    def test_list_hosted_configuration_versions(self, appconfig):
        app_resp = appconfig.create_application(Name=_unique("app"))
        app_id = app_resp["Id"]
        prof_resp = appconfig.create_configuration_profile(
            ApplicationId=app_id,
            Name=_unique("profile"),
            LocationUri="hosted",
        )
        prof_id = prof_resp["Id"]
        try:
            for i in range(3):
                appconfig.create_hosted_configuration_version(
                    ApplicationId=app_id,
                    ConfigurationProfileId=prof_id,
                    Content=f'{{"v": {i}}}'.encode(),
                    ContentType="application/json",
                )

            list_resp = appconfig.list_hosted_configuration_versions(
                ApplicationId=app_id,
                ConfigurationProfileId=prof_id,
            )
            assert "Items" in list_resp
            assert len(list_resp["Items"]) == 3
            versions = [item["VersionNumber"] for item in list_resp["Items"]]
            assert 1 in versions
            assert 2 in versions
            assert 3 in versions
        finally:
            appconfig.delete_configuration_profile(
                ApplicationId=app_id, ConfigurationProfileId=prof_id
            )
            appconfig.delete_application(ApplicationId=app_id)


class TestAppConfigDeploymentOperations:
    """Tests for StartDeployment, GetDeployment, ListDeployments, StopDeployment."""

    def _setup_for_deployment(self, appconfig):
        """Create app, env, profile, version, and strategy for deployment."""
        app_resp = appconfig.create_application(Name=_unique("app"))
        app_id = app_resp["Id"]
        env_resp = appconfig.create_environment(ApplicationId=app_id, Name=_unique("env"))
        env_id = env_resp["Id"]
        prof_resp = appconfig.create_configuration_profile(
            ApplicationId=app_id,
            Name=_unique("profile"),
            LocationUri="hosted",
        )
        prof_id = prof_resp["Id"]
        appconfig.create_hosted_configuration_version(
            ApplicationId=app_id,
            ConfigurationProfileId=prof_id,
            Content=b'{"key": "value"}',
            ContentType="application/json",
        )
        strategy_resp = appconfig.create_deployment_strategy(
            Name=_unique("strategy"),
            DeploymentDurationInMinutes=0,
            GrowthFactor=100.0,
            FinalBakeTimeInMinutes=0,
            ReplicateTo="NONE",
        )
        strategy_id = strategy_resp["Id"]
        return app_id, env_id, prof_id, strategy_id

    def test_start_deployment(self, appconfig):
        app_id, env_id, prof_id, strategy_id = self._setup_for_deployment(appconfig)
        try:
            resp = appconfig.start_deployment(
                ApplicationId=app_id,
                EnvironmentId=env_id,
                DeploymentStrategyId=strategy_id,
                ConfigurationProfileId=prof_id,
                ConfigurationVersion="1",
            )
            assert resp["ApplicationId"] == app_id
            assert resp["EnvironmentId"] == env_id
            assert resp["DeploymentNumber"] >= 1
        finally:
            appconfig.delete_environment(ApplicationId=app_id, EnvironmentId=env_id)
            appconfig.delete_configuration_profile(
                ApplicationId=app_id, ConfigurationProfileId=prof_id
            )
            appconfig.delete_deployment_strategy(DeploymentStrategyId=strategy_id)
            appconfig.delete_application(ApplicationId=app_id)

    def test_get_deployment(self, appconfig):
        app_id, env_id, prof_id, strategy_id = self._setup_for_deployment(appconfig)
        try:
            start_resp = appconfig.start_deployment(
                ApplicationId=app_id,
                EnvironmentId=env_id,
                DeploymentStrategyId=strategy_id,
                ConfigurationProfileId=prof_id,
                ConfigurationVersion="1",
            )
            dep_num = start_resp["DeploymentNumber"]

            resp = appconfig.get_deployment(
                ApplicationId=app_id,
                EnvironmentId=env_id,
                DeploymentNumber=dep_num,
            )
            assert resp["DeploymentNumber"] == dep_num
            assert resp["ApplicationId"] == app_id
        finally:
            appconfig.delete_environment(ApplicationId=app_id, EnvironmentId=env_id)
            appconfig.delete_configuration_profile(
                ApplicationId=app_id, ConfigurationProfileId=prof_id
            )
            appconfig.delete_deployment_strategy(DeploymentStrategyId=strategy_id)
            appconfig.delete_application(ApplicationId=app_id)

    def test_list_deployments(self, appconfig):
        app_id, env_id, prof_id, strategy_id = self._setup_for_deployment(appconfig)
        try:
            appconfig.start_deployment(
                ApplicationId=app_id,
                EnvironmentId=env_id,
                DeploymentStrategyId=strategy_id,
                ConfigurationProfileId=prof_id,
                ConfigurationVersion="1",
            )

            resp = appconfig.list_deployments(
                ApplicationId=app_id,
                EnvironmentId=env_id,
            )
            assert "Items" in resp
            assert len(resp["Items"]) >= 1
        finally:
            appconfig.delete_environment(ApplicationId=app_id, EnvironmentId=env_id)
            appconfig.delete_configuration_profile(
                ApplicationId=app_id, ConfigurationProfileId=prof_id
            )
            appconfig.delete_deployment_strategy(DeploymentStrategyId=strategy_id)
            appconfig.delete_application(ApplicationId=app_id)


class TestAppConfigAccountSettings:
    """Tests for GetAccountSettings and UpdateAccountSettings."""

    def test_get_account_settings(self, appconfig):
        resp = appconfig.get_account_settings()
        # Response should have DeletionProtection key (or similar account-level settings)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestAppConfigValidateConfiguration:
    """Tests for ValidateConfiguration."""

    def test_validate_configuration(self, appconfig):
        app_resp = appconfig.create_application(Name=_unique("app"))
        app_id = app_resp["Id"]
        prof_resp = appconfig.create_configuration_profile(
            ApplicationId=app_id,
            Name=_unique("profile"),
            LocationUri="hosted",
        )
        prof_id = prof_resp["Id"]
        try:
            appconfig.create_hosted_configuration_version(
                ApplicationId=app_id,
                ConfigurationProfileId=prof_id,
                Content=b'{"key": "value"}',
                ContentType="application/json",
            )
            resp = appconfig.validate_configuration(
                ApplicationId=app_id,
                ConfigurationProfileId=prof_id,
                ConfigurationVersion="1",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        finally:
            appconfig.delete_configuration_profile(
                ApplicationId=app_id, ConfigurationProfileId=prof_id
            )
            appconfig.delete_application(ApplicationId=app_id)


class TestAppConfigGetConfiguration:
    """Tests for GetConfiguration (deprecated but still working)."""

    def test_get_configuration(self, appconfig):
        app_resp = appconfig.create_application(Name=_unique("app"))
        app_id = app_resp["Id"]
        env_resp = appconfig.create_environment(ApplicationId=app_id, Name=_unique("env"))
        env_id = env_resp["Id"]
        prof_resp = appconfig.create_configuration_profile(
            ApplicationId=app_id,
            Name=_unique("profile"),
            LocationUri="hosted",
        )
        prof_id = prof_resp["Id"]
        appconfig.create_hosted_configuration_version(
            ApplicationId=app_id,
            ConfigurationProfileId=prof_id,
            Content=b'{"key": "value"}',
            ContentType="application/json",
        )
        try:
            resp = appconfig.get_configuration(
                Application=app_id,
                Environment=env_id,
                Configuration=prof_id,
                ClientId="test-client",
            )
            content = resp["Content"]
            if hasattr(content, "read"):
                content = content.read()
            assert content == b'{"key": "value"}'
            assert resp["ContentType"] == "application/json"
        finally:
            appconfig.delete_configuration_profile(
                ApplicationId=app_id, ConfigurationProfileId=prof_id
            )
            appconfig.delete_environment(ApplicationId=app_id, EnvironmentId=env_id)
            appconfig.delete_application(ApplicationId=app_id)


class TestAppConfigUpdateAccountSettings:
    """Tests for UpdateAccountSettings."""

    def test_update_account_settings(self, appconfig):
        resp = appconfig.update_account_settings(DeletionProtection={"Enabled": False})
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "DeletionProtection" in resp


class TestAppConfigStopDeployment:
    """Tests for StopDeployment."""

    def test_stop_deployment(self, appconfig):
        app_resp = appconfig.create_application(Name=_unique("app"))
        app_id = app_resp["Id"]
        env_resp = appconfig.create_environment(ApplicationId=app_id, Name=_unique("env"))
        env_id = env_resp["Id"]
        prof_resp = appconfig.create_configuration_profile(
            ApplicationId=app_id,
            Name=_unique("profile"),
            LocationUri="hosted",
        )
        prof_id = prof_resp["Id"]
        appconfig.create_hosted_configuration_version(
            ApplicationId=app_id,
            ConfigurationProfileId=prof_id,
            Content=b'{"key": "value"}',
            ContentType="application/json",
        )
        strategy_resp = appconfig.create_deployment_strategy(
            Name=_unique("strategy"),
            DeploymentDurationInMinutes=10,
            GrowthFactor=25.0,
            ReplicateTo="NONE",
        )
        strategy_id = strategy_resp["Id"]
        dep_resp = appconfig.start_deployment(
            ApplicationId=app_id,
            EnvironmentId=env_id,
            DeploymentStrategyId=strategy_id,
            ConfigurationProfileId=prof_id,
            ConfigurationVersion="1",
        )
        dep_num = dep_resp["DeploymentNumber"]
        try:
            resp = appconfig.stop_deployment(
                ApplicationId=app_id,
                EnvironmentId=env_id,
                DeploymentNumber=dep_num,
            )
            assert resp["ApplicationId"] == app_id
            assert resp["State"] == "ROLLED_BACK"
        finally:
            appconfig.delete_environment(ApplicationId=app_id, EnvironmentId=env_id)
            appconfig.delete_configuration_profile(
                ApplicationId=app_id, ConfigurationProfileId=prof_id
            )
            appconfig.delete_deployment_strategy(DeploymentStrategyId=strategy_id)
            appconfig.delete_application(ApplicationId=app_id)


class TestAppConfigUpdateExtensionAssociation:
    """Tests for UpdateExtensionAssociation."""

    def test_update_extension_association(self, appconfig):
        app_resp = appconfig.create_application(Name=_unique("app"))
        app_id = app_resp["Id"]
        ext_resp = appconfig.create_extension(
            Name=_unique("ext"),
            Actions={
                "PRE_CREATE_HOSTED_CONFIGURATION_VERSION": [
                    {
                        "Name": "MyAction",
                        "Uri": "arn:aws:lambda:us-east-1:123456789012:function:my-func",
                    }
                ]
            },
            Parameters={"myParam": {"Required": False}},
        )
        ext_id = ext_resp["Id"]
        app_arn = f"arn:aws:appconfig:us-east-1:123456789012:application/{app_id}"
        assoc_resp = appconfig.create_extension_association(
            ExtensionIdentifier=ext_id,
            ResourceIdentifier=app_arn,
        )
        assoc_id = assoc_resp["Id"]
        try:
            resp = appconfig.update_extension_association(
                ExtensionAssociationId=assoc_id,
                Parameters={"myParam": "updatedValue"},
            )
            assert resp["Id"] == assoc_id
        finally:
            appconfig.delete_extension_association(ExtensionAssociationId=assoc_id)
            appconfig.delete_extension(ExtensionIdentifier=ext_id)
            appconfig.delete_application(ApplicationId=app_id)
