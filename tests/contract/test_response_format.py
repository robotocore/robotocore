"""Contract validation tests for robotocore response format parity.

For each service with a recorded contract, verify that robotocore responses
match the expected structure: response keys, types, metadata format, headers,
and error structure.

These tests run against a live robotocore server on port 4566.
"""

import os
from pathlib import Path

import boto3
import pytest
from botocore.config import Config

from robotocore.testing.contract import AWSContract, load_contracts

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")
CONTRACTS_DIR = Path(__file__).resolve().parent.parent.parent / "contracts"

# Operations that need pre-existing resources — skip in automated tests
SKIP_OPS = {
    ("s3", "GetBucketLocation"),
    ("s3", "HeadBucket"),
    ("dynamodb", "DescribeTable"),
    ("sqs", "GetQueueUrl"),
    ("sqs", "GetQueueAttributes"),
    ("lambda", "GetFunction"),
}


def make_client(service: str):
    config_kwargs = {}
    if service == "s3":
        config_kwargs["s3"] = {"addressing_style": "path"}
    return boto3.client(
        service,
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=Config(**config_kwargs),
    )


def _operation_to_method(operation: str) -> str:
    result = []
    for i, c in enumerate(operation):
        if c.isupper() and i > 0:
            result.append("_")
        result.append(c.lower())
    return "".join(result)


def _collect_contract_params():
    """Collect (service, operation, contract) tuples for parametrize."""
    all_contracts = load_contracts(CONTRACTS_DIR)
    params = []
    for service, contracts in sorted(all_contracts.items()):
        for contract in contracts:
            if (service, contract.operation) in SKIP_OPS:
                continue
            params.append(
                pytest.param(
                    service,
                    contract,
                    id=f"{service}.{contract.operation}",
                )
            )
    return params


contract_params = _collect_contract_params()


@pytest.mark.skipif(not contract_params, reason="No contracts found")
@pytest.mark.parametrize("service,contract", contract_params)
def test_response_keys_match_contract(service, contract):
    """Verify response contains expected keys from the contract."""
    client = make_client(service)
    method = _operation_to_method(contract.operation)
    response = getattr(client, method)()

    actual_keys = set(response.keys()) - {"ResponseMetadata"}
    for key in contract.response_keys:
        assert key in actual_keys, (
            f"{service}.{contract.operation}: missing response key '{key}'. "
            f"Got: {sorted(actual_keys)}"
        )


@pytest.mark.skipif(not contract_params, reason="No contracts found")
@pytest.mark.parametrize("service,contract", contract_params)
def test_response_metadata_present(service, contract):
    """Verify ResponseMetadata exists and has expected keys."""
    client = make_client(service)
    method = _operation_to_method(contract.operation)
    response = getattr(client, method)()

    assert "ResponseMetadata" in response
    metadata = response["ResponseMetadata"]
    assert "HTTPStatusCode" in metadata
    assert "RequestId" in metadata or "HTTPHeaders" in metadata


@pytest.mark.skipif(not contract_params, reason="No contracts found")
@pytest.mark.parametrize("service,contract", contract_params)
def test_response_types_match_contract(service, contract):
    """Verify response value types match the contract."""
    client = make_client(service)
    method = _operation_to_method(contract.operation)
    response = getattr(client, method)()

    result = AWSContract.validate(
        service=service,
        operation=contract.operation,
        response=response,
        headers=dict(response.get("ResponseMetadata", {}).get("HTTPHeaders", {})),
        contract=contract,
    )

    assert not result.wrong_types, (
        f"{service}.{contract.operation}: type mismatches: {result.wrong_types}"
    )


@pytest.mark.skipif(not contract_params, reason="No contracts found")
@pytest.mark.parametrize("service,contract", contract_params)
def test_response_headers(service, contract):
    """Verify expected headers are present in the response."""
    client = make_client(service)
    method = _operation_to_method(contract.operation)
    response = getattr(client, method)()

    metadata = response.get("ResponseMetadata", {})
    headers = dict(metadata.get("HTTPHeaders", {}))
    actual_headers = {k.lower() for k in headers}

    # content-type should always be present
    assert "content-type" in actual_headers, (
        f"{service}.{contract.operation}: missing content-type header. "
        f"Got: {sorted(actual_headers)}"
    )


@pytest.mark.skipif(not contract_params, reason="No contracts found")
@pytest.mark.parametrize("service,contract", contract_params)
def test_full_contract_validation(service, contract):
    """Run full contract validation and report all issues."""
    client = make_client(service)
    method = _operation_to_method(contract.operation)
    response = getattr(client, method)()

    metadata = response.get("ResponseMetadata", {})
    headers = dict(metadata.get("HTTPHeaders", {}))

    result = AWSContract.validate(
        service=service,
        operation=contract.operation,
        response=response,
        headers=headers,
        contract=contract,
    )

    issues = []
    if result.missing_keys:
        issues.append(f"missing_keys={result.missing_keys}")
    if result.wrong_types:
        issues.append(f"wrong_types={result.wrong_types}")
    if result.format_mismatches:
        issues.append(f"format_mismatches={result.format_mismatches}")
    # Note: extra_keys and missing_headers are warnings, not failures
    # AWS may add new keys; our emulator may have different headers

    assert not issues, f"{service}.{contract.operation} contract violations: {'; '.join(issues)}"
