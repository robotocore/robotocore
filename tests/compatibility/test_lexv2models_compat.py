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
        pass


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
