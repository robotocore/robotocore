"""Compatibility tests for Lex V2 Models service."""

import os
import uuid

import boto3
import pytest

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


@pytest.fixture
def lexv2_client():
    return boto3.client(
        "lexv2-models",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def created_bot(lexv2_client):
    """Create a bot and clean it up after the test."""
    bot_name = f"test-bot-{uuid.uuid4().hex[:8]}"
    resp = lexv2_client.create_bot(
        botName=bot_name,
        roleArn="arn:aws:iam::123456789012:role/test",
        dataPrivacy={"childDirected": False},
        idleSessionTTLInSeconds=300,
    )
    bot_id = resp["botId"]
    yield resp
    try:
        lexv2_client.delete_bot(botId=bot_id)
    except Exception:
        pass  # best-effort cleanup


class TestLexV2ModelsCompat:
    def test_list_bots_empty(self, lexv2_client):
        resp = lexv2_client.list_bots()
        assert "botSummaries" in resp
        assert isinstance(resp["botSummaries"], list)

    def test_create_bot(self, lexv2_client):
        bot_name = f"test-bot-{uuid.uuid4().hex[:8]}"
        resp = lexv2_client.create_bot(
            botName=bot_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            dataPrivacy={"childDirected": False},
            idleSessionTTLInSeconds=300,
        )
        bot_id = resp["botId"]
        try:
            assert resp["botName"] == bot_name
            assert "botId" in resp
            assert resp["dataPrivacy"] == {"childDirected": False}
            assert resp["idleSessionTTLInSeconds"] == 300
        finally:
            lexv2_client.delete_bot(botId=bot_id)

    def test_describe_bot(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        resp = lexv2_client.describe_bot(botId=bot_id)
        assert resp["botId"] == bot_id
        assert resp["botName"] == created_bot["botName"]
        assert resp["dataPrivacy"] == {"childDirected": False}
        assert resp["idleSessionTTLInSeconds"] == 300

    def test_update_bot(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        new_name = f"updated-bot-{uuid.uuid4().hex[:8]}"
        resp = lexv2_client.update_bot(
            botId=bot_id,
            botName=new_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            dataPrivacy={"childDirected": False},
            idleSessionTTLInSeconds=600,
        )
        assert resp["botName"] == new_name

        # Verify the update persisted
        desc = lexv2_client.describe_bot(botId=bot_id)
        assert desc["botName"] == new_name
        assert desc["idleSessionTTLInSeconds"] == 600

    def test_list_bots_includes_created(self, lexv2_client, created_bot):
        resp = lexv2_client.list_bots()
        bot_ids = [s["botId"] for s in resp["botSummaries"]]
        assert created_bot["botId"] in bot_ids

    def test_list_bot_aliases(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        resp = lexv2_client.list_bot_aliases(botId=bot_id)
        assert "botAliasSummaries" in resp
        assert isinstance(resp["botAliasSummaries"], list)

    def test_delete_bot(self, lexv2_client):
        bot_name = f"test-bot-{uuid.uuid4().hex[:8]}"
        create_resp = lexv2_client.create_bot(
            botName=bot_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            dataPrivacy={"childDirected": False},
            idleSessionTTLInSeconds=300,
        )
        bot_id = create_resp["botId"]

        del_resp = lexv2_client.delete_bot(botId=bot_id)
        assert del_resp["botId"] == bot_id

        # Verify it no longer appears in list
        bots = lexv2_client.list_bots()
        bot_ids = [s["botId"] for s in bots["botSummaries"]]
        assert bot_id not in bot_ids


@pytest.fixture
def created_alias(lexv2_client, created_bot):
    bot_id = created_bot["botId"]
    resp = lexv2_client.create_bot_alias(
        botAliasName=f"prod-{uuid.uuid4().hex[:8]}",
        description="production alias",
        botVersion="1",
        botId=bot_id,
    )
    yield resp
    try:
        lexv2_client.delete_bot_alias(botAliasId=resp["botAliasId"], botId=bot_id)
    except Exception:
        pass  # best-effort cleanup


class TestBotAliasesCompat:
    def test_bot_create_makes_test_alias(self, lexv2_client, created_bot):
        # moto auto-creates a `test` alias (botVersion "1") with every bot.
        bot_id = created_bot["botId"]
        resp = lexv2_client.list_bot_aliases(botId=bot_id)
        names = [a["botAliasName"] for a in resp["botAliasSummaries"]]
        assert "test" in names

    def test_create_bot_alias(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        alias = lexv2_client.create_bot_alias(
            botAliasName=f"prod-{uuid.uuid4().hex[:8]}",
            description="d",
            botVersion="1",
            botId=bot_id,
            tags={"layer": "alias"},
        )
        try:
            assert alias["botId"] == bot_id
            assert alias["botVersion"] == "1"
            assert alias["botAliasStatus"] == "CREATING"
            assert alias["tags"] == {"layer": "alias"}
        finally:
            lexv2_client.delete_bot_alias(
                botAliasId=alias["botAliasId"], botId=bot_id
            )

    def test_describe_bot_alias_moto_shape(self, lexv2_client, created_alias):
        alias_id = created_alias["botAliasId"]
        resp = lexv2_client.describe_bot_alias(
            botAliasId=alias_id, botId=created_alias["botId"]
        )
        assert resp["botAliasId"] == alias_id
        assert resp["botAliasName"] == created_alias["botAliasName"]
        assert resp["botAliasStatus"] == "CREATING"
        # moto emits the empty history/parent lists on describe
        assert resp["botAliasHistoryEvents"] == []
        assert resp["parentBotNetworks"] == []

    def test_update_bot_alias(self, lexv2_client, created_alias):
        new_name = f"updated-{uuid.uuid4().hex[:8]}"
        resp = lexv2_client.update_bot_alias(
            botAliasId=created_alias["botAliasId"],
            botAliasName=new_name,
            botVersion="1",
            botId=created_alias["botId"],
            description="updated",
        )
        assert resp["botAliasName"] == new_name
        assert resp["description"] == "updated"
        assert lexv2_client.describe_bot_alias(
            botAliasId=created_alias["botAliasId"], botId=created_alias["botId"]
        )["botAliasName"] == new_name

    def test_list_bot_aliases_includes_created(self, lexv2_client, created_alias):
        resp = lexv2_client.list_bot_aliases(botId=created_alias["botId"])
        names = [a["botAliasName"] for a in resp["botAliasSummaries"]]
        assert created_alias["botAliasName"] in names
        assert resp["botId"] == created_alias["botId"]

    def test_delete_bot_alias(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        alias = lexv2_client.create_bot_alias(
            botAliasName=f"short-lived-{uuid.uuid4().hex[:8]}",
            botVersion="1",
            botId=bot_id,
        )
        resp = lexv2_client.delete_bot_alias(
            botAliasId=alias["botAliasId"], botId=bot_id
        )
        assert resp["botAliasId"] == alias["botAliasId"]
        assert resp["botId"] == bot_id
        assert resp["botAliasStatus"] == "CREATING"


class TestTagsCompat:
    def test_create_bot_with_tags(self, lexv2_client):
        # moto echoes supplied botTags back in the create response and
        # stores them under the bot ARN for ListTagsForResource.
        bot_name = f"tagged-bot-{uuid.uuid4().hex[:8]}"
        resp = lexv2_client.create_bot(
            botName=bot_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            dataPrivacy={"childDirected": False},
            idleSessionTTLInSeconds=300,
            botTags={"created": "tagged"},
        )
        try:
            assert resp["botTags"] == {"created": "tagged"}
            arn = f"arn:aws:lex:us-east-1:123456789012:bot/{resp['botId']}"
            assert lexv2_client.list_tags_for_resource(resourceARN=arn)["tags"] == {
                "created": "tagged"
            }
        finally:
            lexv2_client.delete_bot(botId=resp["botId"])

    def test_tag_roundtrip(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        arn = f"arn:aws:lex:us-east-1:123456789012:bot/{bot_id}"
        lexv2_client.tag_resource(resourceARN=arn, tags={"stage": "dev"})
        tags = lexv2_client.list_tags_for_resource(resourceARN=arn)["tags"]
        assert tags["stage"] == "dev"
        lexv2_client.untag_resource(resourceARN=arn, tagKeys=["stage"])
        tags = lexv2_client.list_tags_for_resource(resourceARN=arn)["tags"]
        assert "stage" not in tags


class TestResourcePolicyCompat:
    def test_resource_policy_crud(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        arn = f"arn:aws:lex:us-east-1:123456789012:bot/{bot_id}"
        created = lexv2_client.create_resource_policy(
            resourceArn=arn, policy='{"Statement": []}'
        )
        assert created["resourceArn"] == arn
        revision = created["revisionId"]
        described = lexv2_client.describe_resource_policy(resourceArn=arn)
        assert described["policy"] == '{"Statement": []}'
        assert described["revisionId"] == revision

        updated = lexv2_client.update_resource_policy(
            resourceArn=arn, policy="{}", expectedRevisionId=revision
        )
        assert updated["revisionId"] != revision

        deleted = lexv2_client.delete_resource_policy(
            resourceArn=arn, expectedRevisionId=updated["revisionId"]
        )
        assert deleted["resourceArn"] == arn

    def test_update_resource_policy_revision_mismatch(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        arn = f"arn:aws:lex:us-east-1:123456789012:bot/{bot_id}"
        lexv2_client.create_resource_policy(resourceArn=arn, policy="{}")
        with pytest.raises(
            lexv2_client.exceptions.ConflictException,
        ):
            lexv2_client.update_resource_policy(
                resourceArn=arn, policy="{}", expectedRevisionId="not-the-revision"
            )


class TestMissingResourcesAndGaps:
    def test_describe_missing_bot_not_found(self, lexv2_client):
        import botocore.exceptions

        with pytest.raises(botocore.exceptions.ClientError) as ctx:
            lexv2_client.describe_bot(botId="ffffffff-ffff-ffff-ffff-ffffffffffff")
        error = ctx.value.response["Error"]
        assert error["Code"] == "ResourceNotFoundException"

    # Operations outside moto's lexv2models handler surface (locales, intents,
    # exports, test sets, uploads): moto defines handler methods only for the
    # bot/alias/policy/tag ops exercised above and raises
    # NotImplementedError for the rest; the twin answers them with HTTP 501
    # NotImplemented. Verified against the live twin.
    @pytest.mark.parametrize(
        "op,kwargs",
        [
            (
                "create_bot_locale",
                dict(
                    botId="ffffffff-ffff-ffff-ffff-ffffffffffff",
                    botVersion="DRAFT",
                    localeId="en_US",
                    nluIntentConfidenceThreshold=0.9,
                ),
            ),
            (
                "list_intents",
                dict(
                    botId="ffffffff-ffff-ffff-ffff-ffffffffffff",
                    botVersion="DRAFT",
                    localeId="en_US",
                ),
            ),
            (
                "create_export",
                dict(
                    fileFormat="LexJson",
                    resourceSpecification={
                        "botExportSpecification": {
                            "botId": "ffffffff-ffff-ffff-ffff-ffffffffffff",
                            "botVersion": "DRAFT",
                        }
                    },
                ),
            ),
            ("create_upload_url", {}),
            ("list_test_sets", {}),
        ],
    )
    def test_unimplemented_operations_are_not_implemented(self, lexv2_client, op, kwargs):
        import botocore.exceptions

        with pytest.raises(botocore.exceptions.ClientError) as ctx:
            getattr(lexv2_client, op)(**kwargs)
        error = ctx.value.response["Error"]
        assert error["Code"] == "NotImplemented"
        assert ctx.value.response["ResponseMetadata"]["HTTPStatusCode"] == 501