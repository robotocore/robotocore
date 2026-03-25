"""Compatibility tests for Lex V2 Models service."""

import os
import uuid

import boto3
import pytest

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")

SAMPLE_POLICY_DOC = (
    '{"Version":"2012-10-17","Statement":[{"Effect":"Allow",'
    '"Principal":{"AWS":"arn:aws:iam::123456789012:root"},'
    '"Action":"lex:*","Resource":"*"}]}'
)


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

    def test_create_bot_alias(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        alias_name = f"test-alias-{uuid.uuid4().hex[:8]}"
        resp = lexv2_client.create_bot_alias(
            botAliasName=alias_name,
            botId=bot_id,
        )
        assert resp["botAliasName"] == alias_name
        assert "botAliasId" in resp
        assert resp["botId"] == bot_id

    def test_describe_bot_alias(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        alias_name = f"test-alias-{uuid.uuid4().hex[:8]}"
        create_resp = lexv2_client.create_bot_alias(
            botAliasName=alias_name,
            botId=bot_id,
        )
        alias_id = create_resp["botAliasId"]

        resp = lexv2_client.describe_bot_alias(botAliasId=alias_id, botId=bot_id)
        assert resp["botAliasId"] == alias_id
        assert resp["botAliasName"] == alias_name
        assert resp["botId"] == bot_id

    def test_update_bot_alias(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        alias_name = f"test-alias-{uuid.uuid4().hex[:8]}"
        create_resp = lexv2_client.create_bot_alias(
            botAliasName=alias_name,
            botId=bot_id,
        )
        alias_id = create_resp["botAliasId"]
        new_name = f"updated-alias-{uuid.uuid4().hex[:8]}"

        resp = lexv2_client.update_bot_alias(
            botAliasId=alias_id,
            botAliasName=new_name,
            botId=bot_id,
        )
        assert resp["botAliasId"] == alias_id
        assert resp["botAliasName"] == new_name

    def test_delete_bot_alias(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        alias_name = f"test-alias-{uuid.uuid4().hex[:8]}"
        create_resp = lexv2_client.create_bot_alias(
            botAliasName=alias_name,
            botId=bot_id,
        )
        alias_id = create_resp["botAliasId"]

        del_resp = lexv2_client.delete_bot_alias(botAliasId=alias_id, botId=bot_id)
        assert del_resp["botAliasId"] == alias_id

    def test_create_resource_policy(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        resource_arn = f"arn:aws:lex:us-east-1:123456789012:bot/{bot_id}"
        policy_doc = SAMPLE_POLICY_DOC

        resp = lexv2_client.create_resource_policy(
            resourceArn=resource_arn,
            policy=policy_doc,
        )
        assert resp["resourceArn"] == resource_arn
        assert "revisionId" in resp

    def test_describe_resource_policy(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        resource_arn = f"arn:aws:lex:us-east-1:123456789012:bot/{bot_id}"
        policy_doc = SAMPLE_POLICY_DOC

        lexv2_client.create_resource_policy(
            resourceArn=resource_arn,
            policy=policy_doc,
        )

        resp = lexv2_client.describe_resource_policy(resourceArn=resource_arn)
        assert resp["resourceArn"] == resource_arn
        assert "revisionId" in resp
        assert "policy" in resp

    def test_update_resource_policy(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        resource_arn = f"arn:aws:lex:us-east-1:123456789012:bot/{bot_id}"
        policy_doc = SAMPLE_POLICY_DOC

        create_resp = lexv2_client.create_resource_policy(
            resourceArn=resource_arn,
            policy=policy_doc,
        )
        revision_id = create_resp["revisionId"]

        resp = lexv2_client.update_resource_policy(
            resourceArn=resource_arn,
            policy=policy_doc,
            expectedRevisionId=revision_id,
        )
        assert resp["resourceArn"] == resource_arn
        assert "revisionId" in resp

    def test_delete_resource_policy(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        resource_arn = f"arn:aws:lex:us-east-1:123456789012:bot/{bot_id}"
        policy_doc = SAMPLE_POLICY_DOC

        create_resp = lexv2_client.create_resource_policy(
            resourceArn=resource_arn,
            policy=policy_doc,
        )
        revision_id = create_resp["revisionId"]

        del_resp = lexv2_client.delete_resource_policy(
            resourceArn=resource_arn,
            expectedRevisionId=revision_id,
        )
        assert del_resp["resourceArn"] == resource_arn

    def test_tag_resource(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        resource_arn = f"arn:aws:lex:us-east-1:123456789012:bot/{bot_id}"

        resp = lexv2_client.tag_resource(
            resourceARN=resource_arn,
            tags={"env": "test", "team": "eng"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_tags_for_resource(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        resource_arn = f"arn:aws:lex:us-east-1:123456789012:bot/{bot_id}"

        lexv2_client.tag_resource(
            resourceARN=resource_arn,
            tags={"env": "test", "project": "lex"},
        )

        resp = lexv2_client.list_tags_for_resource(resourceARN=resource_arn)
        assert "tags" in resp
        assert resp["tags"]["env"] == "test"
        assert resp["tags"]["project"] == "lex"

    def test_untag_resource(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        resource_arn = f"arn:aws:lex:us-east-1:123456789012:bot/{bot_id}"

        lexv2_client.tag_resource(
            resourceARN=resource_arn,
            tags={"env": "test", "team": "eng"},
        )

        resp = lexv2_client.untag_resource(
            resourceARN=resource_arn,
            tagKeys=["env"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify tag was removed
        tags_resp = lexv2_client.list_tags_for_resource(resourceARN=resource_arn)
        assert "env" not in tags_resp["tags"]
        assert tags_resp["tags"].get("team") == "eng"
