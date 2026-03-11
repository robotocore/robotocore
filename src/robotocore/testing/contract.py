"""Contract testing framework for validating AWS response format parity.

Captures and validates that robotocore responses match the structure, headers,
and format of real AWS responses. Contracts are derived from botocore service
models and can also be recorded from live AWS responses.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import botocore.session


@dataclass
class Contract:
    """Describes the expected response structure for an AWS operation."""

    service: str
    operation: str
    status_code: int
    response_keys: set[str]
    header_keys: set[str]
    error_format: dict | None
    metadata_keys: set[str]
    timestamp: str
    protocol: str = ""
    key_types: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "operation": self.operation,
            "status_code": self.status_code,
            "response_keys": sorted(self.response_keys),
            "header_keys": sorted(self.header_keys),
            "error_format": self.error_format,
            "metadata_keys": sorted(self.metadata_keys),
            "timestamp": self.timestamp,
            "protocol": self.protocol,
            "key_types": self.key_types,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Contract:
        return cls(
            service=data["service"],
            operation=data["operation"],
            status_code=data["status_code"],
            response_keys=set(data["response_keys"]),
            header_keys=set(data["header_keys"]),
            error_format=data.get("error_format"),
            metadata_keys=set(data["metadata_keys"]),
            timestamp=data["timestamp"],
            protocol=data.get("protocol", ""),
            key_types=data.get("key_types", {}),
        )


@dataclass
class ValidationResult:
    """Result of validating a response against a contract."""

    passed: bool
    missing_keys: list[str] = field(default_factory=list)
    extra_keys: list[str] = field(default_factory=list)
    wrong_types: list[tuple[str, str, str]] = field(default_factory=list)
    missing_headers: list[str] = field(default_factory=list)
    format_mismatches: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "missing_keys": self.missing_keys,
            "extra_keys": self.extra_keys,
            "wrong_types": [{"key": k, "expected": e, "actual": a} for k, e, a in self.wrong_types],
            "missing_headers": self.missing_headers,
            "format_mismatches": self.format_mismatches,
        }


# Standard AWS response headers that should always be present
STANDARD_AWS_HEADERS = {
    "x-amzn-requestid",
    "content-type",
}

# Alternative request ID headers by protocol
REQUEST_ID_HEADERS = {
    "x-amzn-requestid",
    "x-amz-request-id",
    "x-amz-id-2",
}

# UUID pattern for JSON protocol request IDs
UUID_PATTERN = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

# Hex pattern for XML protocol request IDs
HEX_PATTERN = re.compile(r"^[0-9a-fA-F]+$")

# Map Python types to type names used in contracts
TYPE_NAME_MAP = {
    str: "string",
    int: "integer",
    float: "float",
    bool: "boolean",
    list: "list",
    dict: "structure",
    type(None): "null",
}

# Map botocore type names to contract type names
BOTOCORE_TYPE_MAP = {
    "string": "string",
    "integer": "integer",
    "long": "integer",
    "float": "float",
    "double": "float",
    "boolean": "boolean",
    "list": "list",
    "structure": "structure",
    "map": "structure",
    "timestamp": "string",
    "blob": "string",
}


def _python_type_name(value: Any) -> str:
    """Get the contract type name for a Python value."""
    return TYPE_NAME_MAP.get(type(value), "unknown")


def _botocore_type_name(shape_type: str) -> str:
    """Get the contract type name for a botocore shape type."""
    return BOTOCORE_TYPE_MAP.get(shape_type, "unknown")


class AWSContract:
    """Captures and validates AWS response contracts."""

    @staticmethod
    def record(
        service: str,
        operation: str,
        response: dict,
        headers: dict,
        *,
        protocol: str = "",
    ) -> Contract:
        """Record a contract from an actual AWS response.

        Args:
            service: AWS service name (e.g., 's3', 'dynamodb')
            operation: Operation name (e.g., 'ListBuckets', 'ListTables')
            response: The parsed response dict (as returned by boto3)
            headers: The HTTP response headers dict
            protocol: The AWS protocol (json, rest-json, rest-xml, query, ec2)
        """
        # Extract response keys, excluding ResponseMetadata which boto3 adds
        response_keys = set(response.keys()) - {"ResponseMetadata"}

        # Extract key types
        key_types = {}
        for key in response_keys:
            key_types[key] = _python_type_name(response[key])

        # Extract metadata keys
        metadata = response.get("ResponseMetadata", {})
        metadata_keys = set(metadata.keys())

        # Normalize header keys to lowercase
        header_keys = {k.lower() for k in headers}

        # Check for error format
        error_format = None
        if "Error" in response:
            error = response["Error"]
            error_format = {k: type(v).__name__ for k, v in error.items()}

        return Contract(
            service=service,
            operation=operation,
            status_code=metadata.get("HTTPStatusCode", 200),
            response_keys=response_keys,
            header_keys=header_keys,
            error_format=error_format,
            metadata_keys=metadata_keys,
            timestamp=datetime.now(UTC).isoformat(),
            protocol=protocol,
            key_types=key_types,
        )

    @staticmethod
    def validate(
        service: str,
        operation: str,
        response: dict,
        headers: dict,
        contract: Contract,
    ) -> ValidationResult:
        """Validate a response against a recorded contract.

        Args:
            service: AWS service name
            operation: Operation name
            response: The parsed response dict
            headers: The HTTP response headers dict
            contract: The expected contract to validate against

        Returns:
            ValidationResult with pass/fail and details of any mismatches.
        """
        missing_keys: list[str] = []
        extra_keys: list[str] = []
        wrong_types: list[tuple[str, str, str]] = []
        missing_headers: list[str] = []
        format_mismatches: list[str] = []

        # Check response keys
        actual_keys = set(response.keys()) - {"ResponseMetadata"}
        for key in contract.response_keys:
            if key not in actual_keys:
                missing_keys.append(key)

        for key in actual_keys:
            if key not in contract.response_keys:
                extra_keys.append(key)

        # Check types for keys present in both
        for key in actual_keys & contract.response_keys:
            if key in contract.key_types:
                expected_type = contract.key_types[key]
                actual_type = _python_type_name(response[key])
                if expected_type != actual_type:
                    wrong_types.append((key, expected_type, actual_type))

        # Check headers
        actual_headers = {k.lower() for k in headers}
        for header in contract.header_keys:
            if header not in actual_headers:
                missing_headers.append(header)

        # Check ResponseMetadata keys
        metadata = response.get("ResponseMetadata", {})
        actual_meta_keys = set(metadata.keys())
        for key in contract.metadata_keys:
            if key not in actual_meta_keys:
                format_mismatches.append(f"Missing metadata key: {key}")

        # Check error format if contract expects it
        if contract.error_format:
            error = response.get("Error", {})
            if not error:
                format_mismatches.append("Expected error response but got success")
            else:
                for key in contract.error_format:
                    if key not in error:
                        format_mismatches.append(f"Missing error field: {key}")

        # Validate request ID format based on protocol
        _validate_request_id(headers, contract.protocol, format_mismatches)

        passed = (
            not missing_keys
            and not extra_keys
            and not wrong_types
            and not missing_headers
            and not format_mismatches
        )

        return ValidationResult(
            passed=passed,
            missing_keys=sorted(missing_keys),
            extra_keys=sorted(extra_keys),
            wrong_types=sorted(wrong_types),
            missing_headers=sorted(missing_headers),
            format_mismatches=sorted(format_mismatches),
        )

    @staticmethod
    def from_botocore(
        service: str,
        operation: str,
    ) -> Contract:
        """Generate a contract from the botocore service model.

        This derives expected response structure from botocore's service-2.json
        definitions, without needing to call real AWS.

        Args:
            service: AWS service name
            operation: Operation name

        Returns:
            Contract with expected response structure.
        """
        session = botocore.session.get_session()
        model = session.get_service_model(service)
        op_model = model.operation_model(operation)
        protocol = model.protocol

        # Extract output shape members (body keys only)
        response_keys: set[str] = set()
        key_types: dict[str, str] = {}
        if op_model.output_shape and op_model.output_shape.members:
            for name, shape in op_model.output_shape.members.items():
                location = shape.serialization.get("location", "")
                if location in ("header", "headers", "statusCode"):
                    continue
                response_keys.add(name)
                key_types[name] = _botocore_type_name(shape.type_name)

        # Standard headers vary by protocol
        header_keys = _expected_headers_for_protocol(protocol)

        # Standard metadata keys
        metadata_keys = {"RequestId", "HTTPStatusCode", "HTTPHeaders", "RetryAttempts"}

        # Error format depends on protocol
        error_format = _error_format_for_protocol(protocol)

        return Contract(
            service=service,
            operation=operation,
            status_code=200,
            response_keys=response_keys,
            header_keys=header_keys,
            error_format=error_format,
            metadata_keys=metadata_keys,
            timestamp=datetime.now(UTC).isoformat(),
            protocol=protocol,
            key_types=key_types,
        )


def _expected_headers_for_protocol(protocol: str) -> set[str]:
    """Return expected response headers for a given AWS protocol."""
    base = {"content-type"}
    if protocol in ("json", "rest-json"):
        return base | {"x-amzn-requestid"}
    elif protocol in ("rest-xml",):
        return base | {"x-amz-request-id"}
    elif protocol in ("query", "ec2"):
        # Query protocol returns request ID in the response body
        return base
    return base


def _error_format_for_protocol(protocol: str) -> dict:
    """Return expected error response field types for a given AWS protocol."""
    if protocol in ("json", "rest-json"):
        return {"Code": "str", "Message": "str"}
    elif protocol in ("query", "ec2", "rest-xml"):
        return {"Code": "str", "Message": "str", "Type": "str"}
    return {"Code": "str", "Message": "str"}


def _validate_request_id(
    headers: dict,
    protocol: str,
    mismatches: list[str],
) -> None:
    """Validate request ID format matches expectations for the protocol."""
    lower_headers = {k.lower(): v for k, v in headers.items()}

    if protocol in ("json", "rest-json"):
        req_id = lower_headers.get("x-amzn-requestid", "")
        if req_id and not UUID_PATTERN.match(req_id):
            mismatches.append(
                f"Request ID '{req_id}' does not match UUID format for {protocol} protocol"
            )
    elif protocol == "rest-xml":
        req_id = lower_headers.get("x-amz-request-id", "")
        if req_id and not HEX_PATTERN.match(req_id):
            mismatches.append(
                f"Request ID '{req_id}' does not match hex format for {protocol} protocol"
            )


def load_contracts(contracts_dir: Path) -> dict[str, list[Contract]]:
    """Load all contracts from a directory.

    Args:
        contracts_dir: Path to directory containing {service}.json files.

    Returns:
        Dict mapping service name to list of contracts.
    """
    contracts: dict[str, list[Contract]] = {}
    if not contracts_dir.exists():
        return contracts

    for path in sorted(contracts_dir.glob("*.json")):
        service = path.stem
        with open(path) as f:
            data = json.load(f)
        contracts[service] = [Contract.from_dict(op) for op in data.get("operations", [])]

    return contracts


def save_contracts(contracts_dir: Path, service: str, contract_list: list[Contract]) -> Path:
    """Save contracts for a service to a JSON file.

    Args:
        contracts_dir: Path to directory for storing contract files.
        service: AWS service name.
        contract_list: List of contracts to save.

    Returns:
        Path to the written file.
    """
    contracts_dir.mkdir(parents=True, exist_ok=True)
    path = contracts_dir / f"{service}.json"
    data = {
        "service": service,
        "generated_at": datetime.now(UTC).isoformat(),
        "source": "botocore",
        "operations": [c.to_dict() for c in contract_list],
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    return path


def detect_response_format(content_type: str, body: str | bytes) -> str:
    """Detect whether a response is JSON or XML.

    Args:
        content_type: The Content-Type header value.
        body: The response body.

    Returns:
        'json', 'xml', or 'unknown'.
    """
    ct = content_type.lower()
    if "json" in ct:
        return "json"
    if "xml" in ct:
        return "xml"

    # Fallback: inspect body
    if isinstance(body, bytes):
        body = body.decode("utf-8", errors="replace")
    body = body.strip()
    if body.startswith("{") or body.startswith("["):
        return "json"
    if body.startswith("<?xml") or body.startswith("<"):
        return "xml"
    return "unknown"


def validate_xml_structure(body: str | bytes, expected_root: str | None = None) -> list[str]:
    """Validate XML response structure.

    Args:
        body: The XML response body.
        expected_root: Expected root element name, if any.

    Returns:
        List of format mismatch descriptions.
    """
    import xml.etree.ElementTree as ET

    mismatches: list[str] = []
    if isinstance(body, bytes):
        body = body.decode("utf-8", errors="replace")
    try:
        root = ET.fromstring(body)
    except ET.ParseError as e:
        mismatches.append(f"Invalid XML: {e}")
        return mismatches

    if expected_root and not root.tag.endswith(expected_root):
        # Strip namespace prefix for comparison
        local_tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag
        if local_tag != expected_root:
            mismatches.append(f"Expected root element '{expected_root}', got '{local_tag}'")

    return mismatches


def validate_json_structure(body: str | bytes) -> list[str]:
    """Validate JSON response structure.

    Args:
        body: The JSON response body.

    Returns:
        List of format mismatch descriptions.
    """
    mismatches: list[str] = []
    if isinstance(body, bytes):
        body = body.decode("utf-8", errors="replace")
    try:
        json.loads(body)
    except json.JSONDecodeError as e:
        mismatches.append(f"Invalid JSON: {e}")
    return mismatches
