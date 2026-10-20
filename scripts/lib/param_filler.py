"""Auto-fill AWS operation parameters from botocore shapes.

Extracted from scripts/probe_service.py for reuse across validation tools.
"""

import re

import botocore.session

# Hand-tuned params for operations where auto-fill isn't good enough.
KNOWN_PARAMS: dict[str, dict[str, dict]] = {
    "sqs": {
        "CreateQueue": {"QueueName": "probe-test-queue"},
        "GetQueueUrl": {"QueueName": "probe-test-queue"},
    },
    "s3": {
        "CreateBucket": {"Bucket": "probe-test-bucket"},
    },
    "sns": {
        "CreateTopic": {"Name": "probe-test-topic"},
    },
    "sts": {
        "GetCallerIdentity": {},
    },
}


def to_snake_case(name: str) -> str:
    """Convert PascalCase to snake_case."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def build_fake_arn(service: str, resource_type: str = "thing") -> str:
    """Build a syntactically valid fake ARN."""
    return f"arn:aws:{service}:us-east-1:123456789012:{resource_type}/probe-test"


def fill_structure(service_name: str, shape) -> dict | None:
    """Recursively fill a structure shape with minimal valid values."""
    if not hasattr(shape, "members"):
        return {}

    result = {}
    required = set(getattr(shape, "required_members", []))

    for name, member in shape.members.items():
        if name not in required:
            continue

        type_name = member.type_name
        name_lower = name.lower()

        if type_name == "string":
            if "arn" in name_lower:
                result[name] = build_fake_arn(service_name)
            else:
                result[name] = "probe-test-value"
        elif type_name in ("integer", "long"):
            result[name] = 1
        elif type_name == "boolean":
            result[name] = False
        elif type_name == "timestamp":
            result[name] = "2024-01-01T00:00:00Z"
        elif type_name == "blob":
            result[name] = b"test"
        elif type_name == "list":
            result[name] = []
        elif type_name == "map":
            result[name] = {}
        elif type_name == "structure":
            nested = fill_structure(service_name, member)
            if nested is not None:
                result[name] = nested
            else:
                return None
        else:
            return None

    return result


def auto_fill_params(service_name: str, operation_name: str) -> dict | None:
    """Auto-fill required params from botocore shapes.

    Returns a dict of params, or None if we can't fill them.
    """
    session = botocore.session.get_session()
    try:
        model = session.get_service_model(service_name)
        op = model.operation_model(operation_name)
    except Exception:
        return None

    if op.input_shape is None:
        return {}

    params = {}
    required = set(getattr(op.input_shape, "required_members", []))

    for name, shape in op.input_shape.members.items():
        if name not in required:
            continue

        # Skip streaming body params
        if hasattr(shape, "serialization") and shape.serialization.get("streaming"):
            params[name] = b"probe-test-body"
            continue

        type_name = shape.type_name

        if type_name == "string":
            name_lower = name.lower()
            if "arn" in name_lower:
                params[name] = build_fake_arn(service_name)
            elif name_lower in ("bucket", "bucketname"):
                params[name] = "probe-test-bucket"
            elif "name" in name_lower or "id" in name_lower:
                params[name] = "probe-test-value"
            elif "url" in name_lower:
                params[name] = "http://localhost:4566/probe"
            else:
                params[name] = "probe-test-value"
        elif type_name in ("integer", "long"):
            params[name] = 1
        elif type_name == "boolean":
            params[name] = False
        elif type_name == "timestamp":
            params[name] = "2024-01-01T00:00:00Z"
        elif type_name == "blob":
            params[name] = b"probe-test"
        elif type_name == "list":
            params[name] = []
        elif type_name == "map":
            params[name] = {}
        elif type_name == "structure":
            nested = fill_structure(service_name, shape)
            if nested is not None:
                params[name] = nested
            else:
                return None
        else:
            return None

    return params


def get_params_for_operation(service_name: str, operation_name: str) -> dict | None:
    """Get params for an operation, checking KNOWN_PARAMS first, then auto-filling.

    Returns a dict of params, or None if we can't determine valid params.
    """
    known = KNOWN_PARAMS.get(service_name, {})
    if operation_name in known:
        return known[operation_name]
    return auto_fill_params(service_name, operation_name)
