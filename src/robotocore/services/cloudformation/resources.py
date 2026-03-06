"""CloudFormation resource handlers — create/delete AWS resources."""

import json
import uuid

from robotocore.services.cloudformation.engine import CfnResource


def _moto_backend(service: str, account_id: str, region: str):
    """Get a Moto backend instance."""
    from moto.backends import get_backend
    from moto.core import DEFAULT_ACCOUNT_ID

    acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
    backend_dict = get_backend(service)
    return backend_dict[acct][region]


def _moto_global_backend(service: str, account_id: str):
    """Get a Moto backend instance for global services (IAM, S3)."""
    from moto.backends import get_backend
    from moto.core import DEFAULT_ACCOUNT_ID

    acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
    backend_dict = get_backend(service)
    return backend_dict[acct]["global"]


def create_resource(resource: CfnResource, region: str, account_id: str) -> None:
    """Create an AWS resource based on its CloudFormation type."""
    handler = _CREATE_HANDLERS.get(resource.resource_type)
    if handler:
        handler(resource, region, account_id)
    else:
        # Unknown resource type — assign a fake physical ID
        resource.physical_id = (
            f"{resource.resource_type.replace('::', '-').lower()}-{uuid.uuid4().hex[:8]}"
        )
        resource.status = "CREATE_COMPLETE"


def delete_resource(resource: CfnResource, region: str, account_id: str) -> None:
    """Delete an AWS resource."""
    handler = _DELETE_HANDLERS.get(resource.resource_type)
    if handler:
        handler(resource, region, account_id)


# --- SQS ---


def _create_sqs_queue(resource: CfnResource, region: str, account_id: str) -> None:
    from robotocore.services.sqs.provider import _get_store

    store = _get_store(region)
    name = resource.properties.get("QueueName", f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}")
    attrs = {}
    if "VisibilityTimeout" in resource.properties:
        attrs["VisibilityTimeout"] = str(resource.properties["VisibilityTimeout"])
    if "DelaySeconds" in resource.properties:
        attrs["DelaySeconds"] = str(resource.properties["DelaySeconds"])
    if "FifoQueue" in resource.properties:
        attrs["FifoQueue"] = str(resource.properties["FifoQueue"]).lower()
    if "RedrivePolicy" in resource.properties:
        rp = resource.properties["RedrivePolicy"]
        attrs["RedrivePolicy"] = json.dumps(rp) if isinstance(rp, dict) else str(rp)
    queue = store.create_queue(name, region, account_id, attrs)
    resource.physical_id = queue.url
    resource.attributes["Arn"] = queue.arn
    resource.attributes["QueueName"] = queue.name
    resource.attributes["QueueUrl"] = queue.url
    resource.status = "CREATE_COMPLETE"


def _delete_sqs_queue(resource: CfnResource, region: str, account_id: str) -> None:
    from robotocore.services.sqs.provider import _get_store

    store = _get_store(region)
    if resource.physical_id:
        queue = store.get_queue_by_url(resource.physical_id)
        if queue:
            store.delete_queue(queue.name)


# --- SNS ---


def _create_sns_topic(resource: CfnResource, region: str, account_id: str) -> None:
    from robotocore.services.sns.provider import _get_store

    store = _get_store(region)
    name = resource.properties.get("TopicName", f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}")
    topic = store.create_topic(name, region, account_id)
    resource.physical_id = topic.arn
    resource.attributes["TopicArn"] = topic.arn
    resource.attributes["TopicName"] = topic.name
    resource.status = "CREATE_COMPLETE"


def _delete_sns_topic(resource: CfnResource, region: str, account_id: str) -> None:
    from robotocore.services.sns.provider import _get_store

    store = _get_store(region)
    if resource.physical_id:
        store.delete_topic(resource.physical_id)


def _create_sns_subscription(resource: CfnResource, region: str, account_id: str) -> None:
    from robotocore.services.sns.provider import _get_store

    store = _get_store(region)
    topic_arn = resource.properties.get("TopicArn", "")
    protocol = resource.properties.get("Protocol", "")
    endpoint = resource.properties.get("Endpoint", "")
    sub = store.subscribe(topic_arn, protocol, endpoint)
    if sub:
        resource.physical_id = sub.subscription_arn
        resource.attributes["Arn"] = sub.subscription_arn
    resource.status = "CREATE_COMPLETE"


def _delete_sns_subscription(resource: CfnResource, region: str, account_id: str) -> None:
    from robotocore.services.sns.provider import _get_store

    store = _get_store(region)
    if resource.physical_id:
        store.unsubscribe(resource.physical_id)


# --- S3 ---


def _create_s3_bucket(resource: CfnResource, region: str, account_id: str) -> None:
    s3 = _moto_global_backend("s3", account_id)
    name = resource.properties.get(
        "BucketName", f"cfn-{resource.logical_id.lower()}-{uuid.uuid4().hex[:8]}"
    )
    s3.create_bucket(name, region)
    resource.physical_id = name
    resource.attributes["Arn"] = f"arn:aws:s3:::{name}"
    resource.attributes["DomainName"] = f"{name}.s3.amazonaws.com"
    resource.attributes["RegionalDomainName"] = f"{name}.s3.{region}.amazonaws.com"
    resource.status = "CREATE_COMPLETE"


def _delete_s3_bucket(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        s3 = _moto_global_backend("s3", account_id)
        if resource.physical_id:
            s3.delete_bucket(resource.physical_id)
    except Exception:
        pass


# --- IAM Role ---


def _create_iam_role(resource: CfnResource, region: str, account_id: str) -> None:
    iam = _moto_global_backend("iam", account_id)
    name = resource.properties.get("RoleName", f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}")
    policy_doc = resource.properties.get("AssumeRolePolicyDocument", {})
    if isinstance(policy_doc, dict):
        policy_doc = json.dumps(policy_doc)
    path = resource.properties.get("Path", "/")
    role = iam.create_role(
        name,
        policy_doc,
        path,
        permissions_boundary=None,
        description="",
        tags=[],
        max_session_duration=None,
    )
    resource.physical_id = name
    resource.attributes["Arn"] = role.arn
    resource.attributes["RoleId"] = role.id
    resource.status = "CREATE_COMPLETE"


def _delete_iam_role(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        iam = _moto_global_backend("iam", account_id)
        if resource.physical_id:
            iam.delete_role(resource.physical_id)
    except Exception:
        pass


# --- IAM Policy ---


def _create_iam_policy(resource: CfnResource, region: str, account_id: str) -> None:
    iam = _moto_global_backend("iam", account_id)
    name = resource.properties.get(
        "PolicyName", f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}"
    )
    doc = resource.properties.get("PolicyDocument", {})
    if isinstance(doc, dict):
        doc = json.dumps(doc)
    path = resource.properties.get("Path", "/")
    desc = resource.properties.get("Description", "")
    policy = iam.create_policy(desc, path, doc, name, [])
    resource.physical_id = policy.arn
    resource.attributes["Arn"] = policy.arn
    resource.status = "CREATE_COMPLETE"


def _delete_iam_policy(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        iam = _moto_global_backend("iam", account_id)
        if resource.physical_id:
            iam.delete_policy(resource.physical_id)
    except Exception:
        pass


# --- Logs ---


def _create_log_group(resource: CfnResource, region: str, account_id: str) -> None:
    logs = _moto_backend("logs", account_id, region)
    name = resource.properties.get("LogGroupName", f"/cfn/{resource.logical_id}")
    logs.create_log_group(name, {})
    resource.physical_id = name
    resource.attributes["Arn"] = f"arn:aws:logs:{region}:{account_id}:log-group:{name}"
    resource.status = "CREATE_COMPLETE"


def _delete_log_group(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        logs = _moto_backend("logs", account_id, region)
        if resource.physical_id:
            logs.delete_log_group(resource.physical_id)
    except Exception:
        pass


# --- DynamoDB ---


def _create_dynamodb_table(resource: CfnResource, region: str, account_id: str) -> None:
    ddb = _moto_backend("dynamodb", account_id, region)
    name = resource.properties.get("TableName", f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}")
    key_schema = resource.properties.get("KeySchema", [])
    attr_defs = resource.properties.get("AttributeDefinitions", [])
    billing = resource.properties.get("BillingMode", "PROVISIONED")
    throughput = resource.properties.get("ProvisionedThroughput", {})
    gsis = resource.properties.get("GlobalSecondaryIndexes", [])
    lsis = resource.properties.get("LocalSecondaryIndexes", [])
    streams = resource.properties.get("StreamSpecification")
    stream_spec = None
    if streams:
        stream_spec = {
            "StreamEnabled": True,
            "StreamViewType": streams.get("StreamViewType", "NEW_AND_OLD_IMAGES"),
        }
    ddb.create_table(
        name,
        schema=key_schema,
        throughput=throughput,
        attr=attr_defs,
        global_indexes=gsis or None,
        indexes=lsis or None,
        streams=stream_spec,
        billing_mode=billing,
        sse_specification=None,
        tags=[],
        deletion_protection_enabled=None,
        warm_throughput=None,
    )
    resource.physical_id = name
    resource.attributes["Arn"] = f"arn:aws:dynamodb:{region}:{account_id}:table/{name}"
    resource.attributes["TableName"] = name
    resource.status = "CREATE_COMPLETE"


def _delete_dynamodb_table(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        ddb = _moto_backend("dynamodb", account_id, region)
        if resource.physical_id:
            ddb.delete_table(resource.physical_id)
    except Exception:
        pass


# --- Events (EventBridge) ---


def _create_events_rule(resource: CfnResource, region: str, account_id: str) -> None:
    events = _moto_backend("events", account_id, region)
    name = resource.properties.get("Name", f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}")
    event_pattern = resource.properties.get("EventPattern")
    if isinstance(event_pattern, dict):
        event_pattern = json.dumps(event_pattern)
    schedule = resource.properties.get("ScheduleExpression", "")
    state = resource.properties.get("State", "ENABLED")
    desc = resource.properties.get("Description", "")
    event_bus = resource.properties.get("EventBusName", "default")
    events.put_rule(
        name,
        event_pattern=event_pattern,
        scheduled_expression=schedule or None,
        state=state,
        description=desc,
        event_bus_arn=event_bus if event_bus != "default" else None,
    )
    resource.physical_id = name
    resource.attributes["Arn"] = f"arn:aws:events:{region}:{account_id}:rule/{name}"
    resource.status = "CREATE_COMPLETE"


def _delete_events_rule(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        events = _moto_backend("events", account_id, region)
        if resource.physical_id:
            events.delete_rule(resource.physical_id, None)
    except Exception:
        pass


# --- KMS ---


def _create_kms_key(resource: CfnResource, region: str, account_id: str) -> None:
    kms = _moto_backend("kms", account_id, region)
    desc = resource.properties.get("Description", "")
    key_usage = resource.properties.get("KeyUsage", "ENCRYPT_DECRYPT")
    key_spec = resource.properties.get("KeySpec", "SYMMETRIC_DEFAULT")
    policy = resource.properties.get("KeyPolicy", "")
    if isinstance(policy, dict):
        policy = json.dumps(policy)
    key = kms.create_key(policy or None, key_usage, key_spec, desc, None)
    resource.physical_id = key.id
    resource.attributes["Arn"] = key.arn
    resource.attributes["KeyId"] = key.id
    resource.status = "CREATE_COMPLETE"


def _delete_kms_key(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        kms = _moto_backend("kms", account_id, region)
        if resource.physical_id:
            kms.schedule_key_deletion(resource.physical_id, 7)
    except Exception:
        pass


# --- SSM Parameter ---


def _create_ssm_parameter(resource: CfnResource, region: str, account_id: str) -> None:
    ssm = _moto_backend("ssm", account_id, region)
    name = resource.properties.get("Name", f"/cfn/{resource.logical_id}")
    value = resource.properties.get("Value", "")
    param_type = resource.properties.get("Type", "String")
    desc = resource.properties.get("Description", "")
    ssm.put_parameter(name, desc, value, param_type, "", "", False, [], "text", None, None)
    resource.physical_id = name
    resource.attributes["Type"] = param_type
    resource.attributes["Value"] = value
    resource.status = "CREATE_COMPLETE"


def _delete_ssm_parameter(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        ssm = _moto_backend("ssm", account_id, region)
        if resource.physical_id:
            ssm.delete_parameter(resource.physical_id)
    except Exception:
        pass


# --- Lambda ---


def _create_lambda_function(resource: CfnResource, region: str, account_id: str) -> None:
    lmbda = _moto_backend("lambda", account_id, region)
    name = resource.properties.get(
        "FunctionName", f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}"
    )
    runtime = resource.properties.get("Runtime", "python3.12")
    role = resource.properties.get("Role", f"arn:aws:iam::{account_id}:role/cfn-role")
    handler = resource.properties.get("Handler", "index.handler")
    code = resource.properties.get("Code", {})
    desc = resource.properties.get("Description", "")
    timeout = resource.properties.get("Timeout", 3)
    mem = resource.properties.get("MemorySize", 128)
    env_vars = resource.properties.get("Environment", {}).get("Variables", {})
    zip_file = code.get("ZipFile", "")
    if zip_file:
        import base64
        import io
        import zipfile

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("index.py", zip_file)
        code_obj = {"ZipFile": base64.b64encode(buf.getvalue()).decode()}
    else:
        code_obj = {"ZipFile": ""}
    spec = {
        "FunctionName": name,
        "Runtime": runtime,
        "Role": role,
        "Handler": handler,
        "Code": code_obj,
        "Description": desc,
        "Timeout": timeout,
        "MemorySize": mem,
    }
    if env_vars:
        spec["Environment"] = {"Variables": env_vars}
    fn = lmbda.create_function(spec)
    resource.physical_id = fn.function_arn
    resource.attributes["Arn"] = fn.function_arn
    resource.attributes["FunctionName"] = name
    resource.status = "CREATE_COMPLETE"


def _delete_lambda_function(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        lmbda = _moto_backend("lambda", account_id, region)
        if resource.physical_id:
            parts = resource.physical_id.split(":")
            fn_name = parts[-1] if parts else resource.physical_id
            lmbda.delete_function(fn_name)
    except Exception:
        pass


# --- Handler maps ---

_CREATE_HANDLERS = {
    "AWS::SQS::Queue": _create_sqs_queue,
    "AWS::SNS::Topic": _create_sns_topic,
    "AWS::SNS::Subscription": _create_sns_subscription,
    "AWS::S3::Bucket": _create_s3_bucket,
    "AWS::IAM::Role": _create_iam_role,
    "AWS::IAM::Policy": _create_iam_policy,
    "AWS::Logs::LogGroup": _create_log_group,
    "AWS::DynamoDB::Table": _create_dynamodb_table,
    "AWS::Events::Rule": _create_events_rule,
    "AWS::KMS::Key": _create_kms_key,
    "AWS::SSM::Parameter": _create_ssm_parameter,
    "AWS::Lambda::Function": _create_lambda_function,
}

_DELETE_HANDLERS = {
    "AWS::SQS::Queue": _delete_sqs_queue,
    "AWS::SNS::Topic": _delete_sns_topic,
    "AWS::SNS::Subscription": _delete_sns_subscription,
    "AWS::S3::Bucket": _delete_s3_bucket,
    "AWS::IAM::Role": _delete_iam_role,
    "AWS::IAM::Policy": _delete_iam_policy,
    "AWS::Logs::LogGroup": _delete_log_group,
    "AWS::DynamoDB::Table": _delete_dynamodb_table,
    "AWS::Events::Rule": _delete_events_rule,
    "AWS::KMS::Key": _delete_kms_key,
    "AWS::SSM::Parameter": _delete_ssm_parameter,
    "AWS::Lambda::Function": _delete_lambda_function,
}
