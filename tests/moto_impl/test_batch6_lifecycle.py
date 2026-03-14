"""Batch 6 lifecycle tests: IVS, MediaPackage, DataSync, SES."""

import boto3
import pytest

CREDS = {
    "endpoint_url": "http://localhost:4566",
    "aws_access_key_id": "123456789012",
    "aws_secret_access_key": "test",
    "region_name": "us-east-1",
}


@pytest.fixture
def ivs_client():
    return boto3.client("ivs", **CREDS)


@pytest.fixture
def mediapackage_client():
    return boto3.client("mediapackage", **CREDS)


@pytest.fixture
def datasync_client():
    return boto3.client("datasync", **CREDS)


@pytest.fixture
def ses_client():
    return boto3.client("ses", **CREDS)


# ---------------------------------------------------------------------------
# IVS: PlaybackRestrictionPolicy CRUD
# ---------------------------------------------------------------------------


def test_ivs_playback_restriction_policy_lifecycle(ivs_client):
    resp = ivs_client.create_playback_restriction_policy(
        allowedCountries=["US", "CA"],
        allowedOrigins=["https://example.com"],
        name="test-prp-b6",
    )
    policy = resp["playbackRestrictionPolicy"]
    assert policy["name"] == "test-prp-b6"
    assert "arn" in policy
    arn = policy["arn"]

    desc = ivs_client.get_playback_restriction_policy(arn=arn)
    assert desc["playbackRestrictionPolicy"]["name"] == "test-prp-b6"
    assert desc["playbackRestrictionPolicy"]["allowedCountries"] == ["US", "CA"]

    ivs_client.delete_playback_restriction_policy(arn=arn)
    with pytest.raises(Exception):
        ivs_client.get_playback_restriction_policy(arn=arn)


def test_ivs_playback_restriction_policy_not_found(ivs_client):
    with pytest.raises(Exception):
        ivs_client.get_playback_restriction_policy(
            arn="arn:aws:ivs:us-east-1:123456789012:playback-restriction-policy/fake"
        )


# ---------------------------------------------------------------------------
# MediaPackage: HarvestJob + UpdateChannel
# ---------------------------------------------------------------------------


@pytest.fixture
def mp_channel_id(mediapackage_client):
    resp = mediapackage_client.create_channel(Id="test-ch-b6")
    yield resp["Id"]
    try:
        mediapackage_client.delete_channel(Id="test-ch-b6")
    except Exception:
        pass


def test_mediapackage_update_channel(mediapackage_client, mp_channel_id):
    resp = mediapackage_client.update_channel(
        Id=mp_channel_id,
        Description="updated description",
    )
    assert resp["Description"] == "updated description"

    desc = mediapackage_client.describe_channel(Id=mp_channel_id)
    assert desc["Description"] == "updated description"


def test_mediapackage_harvest_job_lifecycle(mediapackage_client, mp_channel_id):
    resp = mediapackage_client.create_origin_endpoint(
        ChannelId=mp_channel_id,
        Id="test-ep-b6",
        ManifestName="index",
        StartoverWindowSeconds=0,
        TimeDelaySeconds=0,
    )
    ep_id = resp["Id"]

    try:
        job = mediapackage_client.create_harvest_job(
            Id="test-hj-b6",
            OriginEndpointId=ep_id,
            StartTime="2025-01-01T00:00:00Z",
            EndTime="2025-01-01T01:00:00Z",
            S3Destination={
                "BucketName": "test-bucket",
                "ManifestKey": "output/manifest.m3u8",
                "RoleArn": "arn:aws:iam::123456789012:role/test-role",
            },
        )
        assert job["Id"] == "test-hj-b6"
        assert "Arn" in job

        desc = mediapackage_client.describe_harvest_job(Id="test-hj-b6")
        assert desc["Id"] == "test-hj-b6"
        assert desc["OriginEndpointId"] == ep_id

        jobs = mediapackage_client.list_harvest_jobs()
        ids = [j["Id"] for j in jobs.get("HarvestJobs", [])]
        assert "test-hj-b6" in ids
    finally:
        try:
            mediapackage_client.delete_origin_endpoint(Id="test-ep-b6")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# DataSync: UpdateAgent + DeleteAgent
# ---------------------------------------------------------------------------


def test_datasync_update_and_delete_agent(datasync_client):
    resp = datasync_client.create_agent(
        ActivationKey="AAAAA-BBBBB-CCCCC-DDDDD-EEEEE",
        AgentName="test-agent-b6",
    )
    agent_arn = resp["AgentArn"]

    datasync_client.update_agent(AgentArn=agent_arn, Name="renamed-agent-b6")
    desc = datasync_client.describe_agent(AgentArn=agent_arn)
    assert desc["Name"] == "renamed-agent-b6"

    datasync_client.delete_agent(AgentArn=agent_arn)
    with pytest.raises(Exception):
        datasync_client.describe_agent(AgentArn=agent_arn)


# ---------------------------------------------------------------------------
# SES: DeleteReceiptRule
# ---------------------------------------------------------------------------


def test_ses_delete_receipt_rule(ses_client):
    ses_client.create_receipt_rule_set(RuleSetName="test-rs-b6")
    ses_client.create_receipt_rule(
        RuleSetName="test-rs-b6",
        Rule={
            "Name": "test-rule-b6",
            "Enabled": True,
            "Actions": [],
            "Recipients": [],
        },
    )
    rs = ses_client.describe_receipt_rule_set(RuleSetName="test-rs-b6")
    rule_names = [r["Name"] for r in rs.get("Rules", [])]
    assert "test-rule-b6" in rule_names

    ses_client.delete_receipt_rule(RuleSetName="test-rs-b6", RuleName="test-rule-b6")

    rs_after = ses_client.describe_receipt_rule_set(RuleSetName="test-rs-b6")
    rule_names_after = [r["Name"] for r in rs_after.get("Rules", [])]
    assert "test-rule-b6" not in rule_names_after
