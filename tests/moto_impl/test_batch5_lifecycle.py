"""Batch 5 lifecycle tests: Athena, Route53Resolver, DataBrew, OpenSearchServerless."""

import json

import boto3
import pytest

CREDS = {
    "endpoint_url": "http://localhost:4566",
    "aws_access_key_id": "123456789012",
    "aws_secret_access_key": "test",
    "region_name": "us-east-1",
}


@pytest.fixture
def athena_client():
    return boto3.client("athena", **CREDS)


@pytest.fixture
def r53r_client():
    return boto3.client("route53resolver", **CREDS)


@pytest.fixture
def databrew_client():
    return boto3.client("databrew", **CREDS)


@pytest.fixture
def oss_client():
    return boto3.client("opensearchserverless", **CREDS)


# ---------------------------------------------------------------------------
# Athena: GetCapacityAssignmentConfiguration
# ---------------------------------------------------------------------------


def test_athena_get_capacity_assignment_configuration(athena_client):
    resp = athena_client.get_capacity_assignment_configuration(
        CapacityReservationName="test-reservation",
    )
    config = resp["CapacityAssignmentConfiguration"]
    assert config["CapacityReservationName"] == "test-reservation"
    assert isinstance(config["CapacityAssignments"], list)


# ---------------------------------------------------------------------------
# Route53Resolver: Policy operations
# ---------------------------------------------------------------------------


@pytest.fixture
def firewall_rule_group_arn(r53r_client):
    resp = r53r_client.create_firewall_rule_group(
        Name="test-frg-b5",
        CreatorRequestId="b5-frg",
    )
    arn = resp["FirewallRuleGroup"]["Arn"]
    yield arn
    try:
        r53r_client.delete_firewall_rule_group(FirewallRuleGroupId=resp["FirewallRuleGroup"]["Id"])
    except Exception:
        pass


def test_r53r_firewall_rule_group_policy(r53r_client, firewall_rule_group_arn):
    policy = json.dumps({"Version": "2012-10-17", "Statement": []})
    r53r_client.put_firewall_rule_group_policy(
        Arn=firewall_rule_group_arn,
        FirewallRuleGroupPolicy=policy,
    )
    resp = r53r_client.get_firewall_rule_group_policy(
        Arn=firewall_rule_group_arn,
    )
    assert resp["FirewallRuleGroupPolicy"] == policy


def test_r53r_resolver_rule_policy(r53r_client):
    fake_arn = "arn:aws:route53resolver:us-east-1:123456789012:resolver-rule/rslvr-rr-fake"
    policy = json.dumps({"Version": "2012-10-17", "Statement": []})
    r53r_client.put_resolver_rule_policy(
        Arn=fake_arn,
        ResolverRulePolicy=policy,
    )
    resp = r53r_client.get_resolver_rule_policy(Arn=fake_arn)
    assert resp["ResolverRulePolicy"] == policy


# ---------------------------------------------------------------------------
# DataBrew: Project CRUD
# ---------------------------------------------------------------------------


def test_databrew_project_lifecycle(databrew_client):
    databrew_client.create_dataset(
        Name="test-ds-b5",
        Input={"S3InputDefinition": {"Bucket": "fake", "Key": "fake.csv"}},
        FormatOptions={"Json": {"MultiLine": False}},
    )
    try:
        resp = databrew_client.create_project(
            Name="test-proj-b5",
            DatasetName="test-ds-b5",
            RecipeName="test-recipe-b5",
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        assert resp["Name"] == "test-proj-b5"

        desc = databrew_client.describe_project(Name="test-proj-b5")
        assert desc["Name"] == "test-proj-b5"
        assert desc["DatasetName"] == "test-ds-b5"

        projects = databrew_client.list_projects()
        names = [p["Name"] for p in projects["Projects"]]
        assert "test-proj-b5" in names

        databrew_client.delete_project(Name="test-proj-b5")
        with pytest.raises(Exception):
            databrew_client.describe_project(Name="test-proj-b5")
    finally:
        try:
            databrew_client.delete_dataset(Name="test-ds-b5")
        except Exception:
            pass


def test_databrew_project_not_found(databrew_client):
    with pytest.raises(Exception) as exc_info:
        databrew_client.describe_project(Name="nonexistent-proj")
    assert "not found" in str(exc_info.value).lower() or "ResourceNotFound" in str(exc_info.value)


# ---------------------------------------------------------------------------
# DataBrew: Schedule CRUD
# ---------------------------------------------------------------------------


def test_databrew_schedule_lifecycle(databrew_client):
    resp = databrew_client.create_schedule(
        Name="test-sched-b5",
        CronExpression="cron(0 12 * * ? *)",
    )
    assert resp["Name"] == "test-sched-b5"

    desc = databrew_client.describe_schedule(Name="test-sched-b5")
    assert desc["Name"] == "test-sched-b5"
    assert desc["CronExpression"] == "cron(0 12 * * ? *)"

    schedules = databrew_client.list_schedules()
    names = [s["Name"] for s in schedules["Schedules"]]
    assert "test-sched-b5" in names

    databrew_client.delete_schedule(Name="test-sched-b5")
    with pytest.raises(Exception):
        databrew_client.describe_schedule(Name="test-sched-b5")


def test_databrew_schedule_not_found(databrew_client):
    with pytest.raises(Exception) as exc_info:
        databrew_client.describe_schedule(Name="nonexistent-sched")
    assert "not found" in str(exc_info.value).lower() or "ResourceNotFound" in str(exc_info.value)


# ---------------------------------------------------------------------------
# OpenSearchServerless: Index CRUD
# ---------------------------------------------------------------------------


@pytest.fixture
def oss_collection_id(oss_client):
    oss_client.create_security_policy(
        name="test-enc-b5",
        type="encryption",
        policy=json.dumps(
            {
                "Rules": [
                    {
                        "ResourceType": "collection",
                        "Resource": ["collection/test-coll-b5"],
                    }
                ],
                "AWSOwnedKey": True,
            }
        ),
    )
    resp = oss_client.create_collection(
        name="test-coll-b5",
        type="SEARCH",
    )
    coll_id = resp["createCollectionDetail"]["id"]
    yield coll_id
    try:
        oss_client.delete_collection(id=coll_id)
    except Exception:
        pass
    try:
        oss_client.delete_security_policy(name="test-enc-b5", type="encryption")
    except Exception:
        pass


def test_oss_index_lifecycle(oss_client, oss_collection_id):
    resp = oss_client.create_index(
        id=oss_collection_id,
        indexName="test-idx-b5",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    desc = oss_client.get_index(id=oss_collection_id, indexName="test-idx-b5")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    oss_client.delete_index(id=oss_collection_id, indexName="test-idx-b5")
    assert desc["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_oss_index_not_found(oss_client):
    with pytest.raises(Exception):
        oss_client.get_index(id="nonexistent-idx")
