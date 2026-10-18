"""Shared fixtures for the moto_impl lifecycle suites.

The suites hardcode ``http://localhost:4566`` so they run against a default
container. When a parity lane sets ``ENDPOINT_URL`` those endpoints are
rewritten to it, so the same suites can run against lane ports.
"""

import os

import boto3

_ENDPOINT = os.environ.get("ENDPOINT_URL")


if _ENDPOINT:

    def _rewritten(kwargs: dict) -> dict:
        endpoint = kwargs.get("endpoint_url")
        if isinstance(endpoint, str) and endpoint.startswith("http://localhost:4566"):
            kwargs["endpoint_url"] = _ENDPOINT
        return kwargs

    _orig_client = boto3.client
    _orig_resource = boto3.resource

    def _patched_client(service_name, *args, **kwargs):
        return _orig_client(service_name, *args, **_rewritten(kwargs))

    def _patched_resource(service_name, *args, **kwargs):
        return _orig_resource(service_name, *args, **_rewritten(kwargs))

    boto3.client = _patched_client
    boto3.resource = _patched_resource
