"""Shared fixtures for compatibility tests."""

import os

import boto3
from botocore.config import Config

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


def make_client(service_name: str, **kwargs):
    config_kwargs = {}
    if service_name == "s3":
        config_kwargs["s3"] = {"addressing_style": "path"}

    return boto3.client(
        service_name,
        endpoint_url=ENDPOINT_URL,
        region_name=kwargs.pop("region_name", "us-east-1"),
        aws_access_key_id=kwargs.pop("aws_access_key_id", "testing"),
        aws_secret_access_key=kwargs.pop("aws_secret_access_key", "testing"),
        config=Config(**config_kwargs),
        **kwargs,
    )
