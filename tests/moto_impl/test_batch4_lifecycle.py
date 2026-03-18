"""Lifecycle tests for Batch 4 operations.

IoT: DescribeEncryptionConfiguration, GetThingConnectivityData
Config: DescribeConfigurationAggregatorSourcesStatus
Rekognition: ListUsers
ECR: GetSigningConfiguration
"""

import logging

import boto3
import pytest
from botocore.exceptions import ClientError

ENDPOINT = "http://localhost:4566"
CREDS = {
    "endpoint_url": ENDPOINT,
    "region_name": "us-east-1",
    "aws_access_key_id": "testing",
    "aws_secret_access_key": "testing",
}


# --- IoT ---


@pytest.fixture
def iot_client():
    return boto3.client("iot", **CREDS)


def test_iot_describe_encryption_configuration(iot_client):
    """DescribeEncryptionConfiguration returns encryption settings."""
    resp = iot_client.describe_encryption_configuration()
    assert resp["encryptionType"] in ("AWS_OWNED_KEY", "CUSTOMER_MANAGED")


def test_iot_get_thing_connectivity_data(iot_client):
    """GetThingConnectivityData returns connectivity for a thing."""
    iot_client.create_thing(thingName="test-conn-thing")
    try:
        resp = iot_client.get_thing_connectivity_data(thingName="test-conn-thing")
        assert "connected" in resp
        assert isinstance(resp["connected"], bool)
    finally:
        try:
            iot_client.delete_thing(thingName="test-conn-thing")
        except ClientError as e:
            logging.debug("pre-cleanup skipped: %s", e)


def test_iot_get_thing_connectivity_data_not_found(iot_client):
    """GetThingConnectivityData raises for nonexistent thing."""
    with pytest.raises(iot_client.exceptions.ResourceNotFoundException):
        iot_client.get_thing_connectivity_data(thingName="nonexistent-thing-12345")


# --- Config ---


@pytest.fixture
def config_client():
    return boto3.client("config", **CREDS)


def test_config_describe_aggregator_sources_status(config_client):
    """DescribeConfigurationAggregatorSourcesStatus works."""
    config_client.put_configuration_aggregator(
        ConfigurationAggregatorName="test-agg-b4",
        AccountAggregationSources=[
            {
                "AccountIds": ["123456789012"],
                "AllAwsRegions": True,
            }
        ],
    )
    try:
        resp = config_client.describe_configuration_aggregator_sources_status(
            ConfigurationAggregatorName="test-agg-b4"
        )
        assert "AggregatedSourceStatusList" in resp
        assert isinstance(resp["AggregatedSourceStatusList"], list)
    finally:
        try:
            config_client.delete_configuration_aggregator(ConfigurationAggregatorName="test-agg-b4")
        except ClientError as e:
            logging.debug("pre-cleanup skipped: %s", e)


def test_config_describe_aggregator_not_found(config_client):
    """Nonexistent aggregator raises exception."""
    with pytest.raises(Exception) as exc:
        config_client.describe_configuration_aggregator_sources_status(
            ConfigurationAggregatorName="nonexistent-agg-12345"
        )
    assert "NoSuchConfigurationAggregator" in str(exc.value.response["Error"]["Code"])


# --- Rekognition ---


@pytest.fixture
def rek_client():
    return boto3.client("rekognition", **CREDS)


def test_rekognition_list_users(rek_client):
    """ListUsers returns users for a collection."""
    rek_client.create_collection(CollectionId="test-coll-b4")
    try:
        resp = rek_client.list_users(CollectionId="test-coll-b4")
        assert "Users" in resp
        assert isinstance(resp["Users"], list)
    finally:
        try:
            rek_client.delete_collection(CollectionId="test-coll-b4")
        except ClientError as e:
            logging.debug("pre-cleanup skipped: %s", e)


def test_rekognition_list_users_not_found(rek_client):
    """ListUsers raises for nonexistent collection."""
    with pytest.raises(rek_client.exceptions.ResourceNotFoundException):
        rek_client.list_users(CollectionId="nonexistent-coll-12345")


# --- ECR ---


@pytest.fixture
def ecr_client():
    return boto3.client("ecr", **CREDS)


def test_ecr_get_signing_configuration(ecr_client):
    """GetSigningConfiguration returns registry signing config."""
    resp = ecr_client.get_signing_configuration()
    assert "signingConfiguration" in resp
    assert isinstance(resp["signingConfiguration"]["rules"], list)
    assert "registryId" in resp
