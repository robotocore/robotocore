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


@pytest.fixture
def bot_with_locale(lexv2_client, created_bot):
    """Create a bot with a locale."""
    bot_id = created_bot["botId"]
    resp = lexv2_client.create_bot_locale(
        botId=bot_id,
        botVersion="DRAFT",
        localeId="en_US",
        nluIntentConfidenceThreshold=0.4,
    )
    yield {"bot": created_bot, "locale": resp}


@pytest.fixture
def bot_with_intent(lexv2_client, bot_with_locale):
    """Create a bot with a locale and intent."""
    bot_id = bot_with_locale["bot"]["botId"]
    resp = lexv2_client.create_intent(
        botId=bot_id,
        botVersion="DRAFT",
        localeId="en_US",
        intentName=f"TestIntent{uuid.uuid4().hex[:8]}",
    )
    yield {"bot": bot_with_locale["bot"], "locale": bot_with_locale["locale"], "intent": resp}


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


class TestBotLocaleCompat:
    def test_create_bot_locale(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        resp = lexv2_client.create_bot_locale(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            nluIntentConfidenceThreshold=0.4,
        )
        assert resp["localeId"] == "en_US"
        assert resp["botId"] == bot_id
        assert resp["botVersion"] == "DRAFT"
        assert "botLocaleStatus" in resp

    def test_describe_bot_locale(self, lexv2_client, bot_with_locale):
        bot_id = bot_with_locale["bot"]["botId"]
        resp = lexv2_client.describe_bot_locale(botId=bot_id, botVersion="DRAFT", localeId="en_US")
        assert resp["localeId"] == "en_US"
        assert resp["botId"] == bot_id
        assert "nluIntentConfidenceThreshold" in resp

    def test_list_bot_locales(self, lexv2_client, bot_with_locale):
        bot_id = bot_with_locale["bot"]["botId"]
        resp = lexv2_client.list_bot_locales(botId=bot_id, botVersion="DRAFT")
        assert "botLocaleSummaries" in resp
        locale_ids = [s["localeId"] for s in resp["botLocaleSummaries"]]
        assert "en_US" in locale_ids

    def test_update_bot_locale(self, lexv2_client, bot_with_locale):
        bot_id = bot_with_locale["bot"]["botId"]
        resp = lexv2_client.update_bot_locale(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            nluIntentConfidenceThreshold=0.7,
        )
        assert resp["localeId"] == "en_US"

        # Verify update persisted
        desc = lexv2_client.describe_bot_locale(botId=bot_id, botVersion="DRAFT", localeId="en_US")
        assert desc["nluIntentConfidenceThreshold"] == 0.7

    def test_build_bot_locale(self, lexv2_client, bot_with_locale):
        bot_id = bot_with_locale["bot"]["botId"]
        resp = lexv2_client.build_bot_locale(botId=bot_id, botVersion="DRAFT", localeId="en_US")
        assert resp["localeId"] == "en_US"
        assert "botLocaleStatus" in resp

    def test_delete_bot_locale(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        lexv2_client.create_bot_locale(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="es_ES",
            nluIntentConfidenceThreshold=0.5,
        )
        resp = lexv2_client.delete_bot_locale(botId=bot_id, botVersion="DRAFT", localeId="es_ES")
        assert resp["localeId"] == "es_ES"

        # Verify it was deleted
        locales = lexv2_client.list_bot_locales(botId=bot_id, botVersion="DRAFT")
        locale_ids = [s["localeId"] for s in locales["botLocaleSummaries"]]
        assert "es_ES" not in locale_ids


class TestBotVersionCompat:
    def test_create_bot_version(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        resp = lexv2_client.create_bot_version(
            botId=bot_id,
            botVersionLocaleSpecification={"en_US": {"sourceBotVersion": "DRAFT"}},
        )
        assert "botVersion" in resp
        assert resp["botId"] == bot_id

    def test_list_bot_versions(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        lexv2_client.create_bot_version(
            botId=bot_id,
            botVersionLocaleSpecification={"en_US": {"sourceBotVersion": "DRAFT"}},
        )
        resp = lexv2_client.list_bot_versions(botId=bot_id)
        assert "botVersionSummaries" in resp
        assert len(resp["botVersionSummaries"]) >= 1

    def test_describe_bot_version(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        create_resp = lexv2_client.create_bot_version(
            botId=bot_id,
            botVersionLocaleSpecification={"en_US": {"sourceBotVersion": "DRAFT"}},
        )
        version = create_resp["botVersion"]

        resp = lexv2_client.describe_bot_version(botId=bot_id, botVersion=version)
        assert resp["botId"] == bot_id
        assert resp["botVersion"] == version
        assert "botStatus" in resp

    def test_delete_bot_version(self, lexv2_client, created_bot):
        bot_id = created_bot["botId"]
        create_resp = lexv2_client.create_bot_version(
            botId=bot_id,
            botVersionLocaleSpecification={"en_US": {"sourceBotVersion": "DRAFT"}},
        )
        version = create_resp["botVersion"]

        del_resp = lexv2_client.delete_bot_version(botId=bot_id, botVersion=version)
        assert del_resp["botId"] == bot_id
        assert del_resp["botVersion"] == version


class TestIntentCompat:
    def test_create_intent(self, lexv2_client, bot_with_locale):
        bot_id = bot_with_locale["bot"]["botId"]
        intent_name = f"TestIntent{uuid.uuid4().hex[:8]}"
        resp = lexv2_client.create_intent(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            intentName=intent_name,
        )
        assert resp["intentId"]
        assert resp["intentName"] == intent_name
        assert resp["botId"] == bot_id

    def test_describe_intent(self, lexv2_client, bot_with_intent):
        bot_id = bot_with_intent["bot"]["botId"]
        intent_id = bot_with_intent["intent"]["intentId"]
        resp = lexv2_client.describe_intent(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            intentId=intent_id,
        )
        assert resp["intentId"] == intent_id
        assert resp["botId"] == bot_id
        assert "intentName" in resp

    def test_update_intent(self, lexv2_client, bot_with_intent):
        bot_id = bot_with_intent["bot"]["botId"]
        intent_id = bot_with_intent["intent"]["intentId"]
        new_name = f"Updated{uuid.uuid4().hex[:8]}"
        resp = lexv2_client.update_intent(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            intentId=intent_id,
            intentName=new_name,
        )
        assert resp["intentName"] == new_name

        # Verify persisted
        desc = lexv2_client.describe_intent(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            intentId=intent_id,
        )
        assert desc["intentName"] == new_name

    def test_list_intents(self, lexv2_client, bot_with_intent):
        bot_id = bot_with_intent["bot"]["botId"]
        intent_id = bot_with_intent["intent"]["intentId"]
        resp = lexv2_client.list_intents(botId=bot_id, botVersion="DRAFT", localeId="en_US")
        assert "intentSummaries" in resp
        intent_ids = [s["intentId"] for s in resp["intentSummaries"]]
        assert intent_id in intent_ids

    def test_delete_intent(self, lexv2_client, bot_with_locale):
        bot_id = bot_with_locale["bot"]["botId"]
        intent_name = f"DeleteMe{uuid.uuid4().hex[:8]}"
        create_resp = lexv2_client.create_intent(
            botId=bot_id, botVersion="DRAFT", localeId="en_US", intentName=intent_name
        )
        intent_id = create_resp["intentId"]

        lexv2_client.delete_intent(
            botId=bot_id, botVersion="DRAFT", localeId="en_US", intentId=intent_id
        )

        # Verify it was deleted
        intents = lexv2_client.list_intents(botId=bot_id, botVersion="DRAFT", localeId="en_US")
        intent_ids = [s["intentId"] for s in intents["intentSummaries"]]
        assert intent_id not in intent_ids


class TestSlotCompat:
    def test_create_slot(self, lexv2_client, bot_with_intent):
        bot_id = bot_with_intent["bot"]["botId"]
        intent_id = bot_with_intent["intent"]["intentId"]
        resp = lexv2_client.create_slot(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            intentId=intent_id,
            slotName="MySlot",
            slotTypeId="AMAZON.AlphaNumeric",
            valueElicitationSetting={"slotConstraint": "Required"},
        )
        assert "slotId" in resp
        assert resp["slotName"] == "MySlot"
        assert resp["intentId"] == intent_id

    def test_describe_slot(self, lexv2_client, bot_with_intent):
        bot_id = bot_with_intent["bot"]["botId"]
        intent_id = bot_with_intent["intent"]["intentId"]
        create_resp = lexv2_client.create_slot(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            intentId=intent_id,
            slotName="DescSlot",
            slotTypeId="AMAZON.AlphaNumeric",
            valueElicitationSetting={"slotConstraint": "Required"},
        )
        slot_id = create_resp["slotId"]

        resp = lexv2_client.describe_slot(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            intentId=intent_id,
            slotId=slot_id,
        )
        assert resp["slotId"] == slot_id
        assert resp["slotName"] == "DescSlot"

    def test_list_slots(self, lexv2_client, bot_with_intent):
        bot_id = bot_with_intent["bot"]["botId"]
        intent_id = bot_with_intent["intent"]["intentId"]
        lexv2_client.create_slot(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            intentId=intent_id,
            slotName="ListSlot",
            slotTypeId="AMAZON.AlphaNumeric",
            valueElicitationSetting={"slotConstraint": "Required"},
        )

        resp = lexv2_client.list_slots(
            botId=bot_id, botVersion="DRAFT", localeId="en_US", intentId=intent_id
        )
        assert "slotSummaries" in resp
        slot_names = [s["slotName"] for s in resp["slotSummaries"]]
        assert "ListSlot" in slot_names

    def test_update_slot(self, lexv2_client, bot_with_intent):
        bot_id = bot_with_intent["bot"]["botId"]
        intent_id = bot_with_intent["intent"]["intentId"]
        create_resp = lexv2_client.create_slot(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            intentId=intent_id,
            slotName="UpdateMe",
            slotTypeId="AMAZON.AlphaNumeric",
            valueElicitationSetting={"slotConstraint": "Required"},
        )
        slot_id = create_resp["slotId"]

        resp = lexv2_client.update_slot(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            intentId=intent_id,
            slotId=slot_id,
            slotName="UpdatedSlot",
            slotTypeId="AMAZON.AlphaNumeric",
            valueElicitationSetting={"slotConstraint": "Optional"},
        )
        assert resp["slotName"] == "UpdatedSlot"

    def test_delete_slot(self, lexv2_client, bot_with_intent):
        bot_id = bot_with_intent["bot"]["botId"]
        intent_id = bot_with_intent["intent"]["intentId"]
        create_resp = lexv2_client.create_slot(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            intentId=intent_id,
            slotName="DeleteSlot",
            slotTypeId="AMAZON.AlphaNumeric",
            valueElicitationSetting={"slotConstraint": "Required"},
        )
        slot_id = create_resp["slotId"]

        lexv2_client.delete_slot(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            intentId=intent_id,
            slotId=slot_id,
        )

        slots = lexv2_client.list_slots(
            botId=bot_id, botVersion="DRAFT", localeId="en_US", intentId=intent_id
        )
        slot_ids = [s["slotId"] for s in slots["slotSummaries"]]
        assert slot_id not in slot_ids


class TestSlotTypeCompat:
    def test_create_slot_type(self, lexv2_client, bot_with_locale):
        bot_id = bot_with_locale["bot"]["botId"]
        resp = lexv2_client.create_slot_type(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            slotTypeName="MySlotType",
            valueSelectionSetting={"resolutionStrategy": "OriginalValue"},
        )
        assert "slotTypeId" in resp
        assert resp["slotTypeName"] == "MySlotType"

    def test_describe_slot_type(self, lexv2_client, bot_with_locale):
        bot_id = bot_with_locale["bot"]["botId"]
        create_resp = lexv2_client.create_slot_type(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            slotTypeName="DescST",
            valueSelectionSetting={"resolutionStrategy": "OriginalValue"},
        )
        slot_type_id = create_resp["slotTypeId"]

        resp = lexv2_client.describe_slot_type(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            slotTypeId=slot_type_id,
        )
        assert resp["slotTypeId"] == slot_type_id
        assert resp["slotTypeName"] == "DescST"

    def test_list_slot_types(self, lexv2_client, bot_with_locale):
        bot_id = bot_with_locale["bot"]["botId"]
        lexv2_client.create_slot_type(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            slotTypeName="ListST",
            valueSelectionSetting={"resolutionStrategy": "OriginalValue"},
        )
        resp = lexv2_client.list_slot_types(botId=bot_id, botVersion="DRAFT", localeId="en_US")
        assert "slotTypeSummaries" in resp
        names = [s["slotTypeName"] for s in resp["slotTypeSummaries"]]
        assert "ListST" in names

    def test_update_slot_type(self, lexv2_client, bot_with_locale):
        bot_id = bot_with_locale["bot"]["botId"]
        create_resp = lexv2_client.create_slot_type(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            slotTypeName="UpdateST",
            valueSelectionSetting={"resolutionStrategy": "OriginalValue"},
        )
        slot_type_id = create_resp["slotTypeId"]

        resp = lexv2_client.update_slot_type(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            slotTypeId=slot_type_id,
            slotTypeName="UpdatedST",
            valueSelectionSetting={"resolutionStrategy": "TopResolution"},
        )
        assert resp["slotTypeName"] == "UpdatedST"

    def test_delete_slot_type(self, lexv2_client, bot_with_locale):
        bot_id = bot_with_locale["bot"]["botId"]
        create_resp = lexv2_client.create_slot_type(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            slotTypeName="DeleteST",
            valueSelectionSetting={"resolutionStrategy": "OriginalValue"},
        )
        slot_type_id = create_resp["slotTypeId"]

        lexv2_client.delete_slot_type(
            botId=bot_id,
            botVersion="DRAFT",
            localeId="en_US",
            slotTypeId=slot_type_id,
        )

        sts = lexv2_client.list_slot_types(botId=bot_id, botVersion="DRAFT", localeId="en_US")
        st_ids = [s["slotTypeId"] for s in sts["slotTypeSummaries"]]
        assert slot_type_id not in st_ids
