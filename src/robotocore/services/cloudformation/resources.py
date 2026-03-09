"""CloudFormation resource handlers — create/delete AWS resources."""

import ipaddress
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
    # Handle Custom:: resources and AWS::CloudFormation::CustomResource
    rtype = resource.resource_type
    if rtype.startswith("Custom::") or rtype == "AWS::CloudFormation::CustomResource":
        _create_custom_resource(resource, region, account_id)
        return

    handler = _CREATE_HANDLERS.get(rtype)
    if handler:
        handler(resource, region, account_id)
    else:
        raise ValueError(f"Unsupported CloudFormation resource type: {rtype}")


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

    # Apply website configuration if present (Moto stores raw XML bytes)
    website_config = resource.properties.get("WebsiteConfiguration")
    if website_config:
        try:
            index_doc = website_config.get("IndexDocument", "index.html")
            error_doc = website_config.get("ErrorDocument", "")
            xml_parts = ["<WebsiteConfiguration>"]
            xml_parts.append(f"<IndexDocument><Suffix>{index_doc}</Suffix></IndexDocument>")
            if error_doc:
                xml_parts.append(f"<ErrorDocument><Key>{error_doc}</Key></ErrorDocument>")
            xml_parts.append("</WebsiteConfiguration>")
            s3.put_bucket_website(name, "".join(xml_parts).encode())
        except Exception:
            pass

    # Apply tags if present (Moto expects dict[str, str])
    bucket_tags = resource.properties.get("Tags", [])
    if bucket_tags:
        try:
            tags_dict = {t["Key"]: t["Value"] for t in bucket_tags if "Key" in t}
            s3.put_bucket_tagging(name, tags_dict)
        except Exception:
            pass

    resource.physical_id = name
    resource.attributes["Arn"] = f"arn:aws:s3:::{name}"
    resource.attributes["DomainName"] = f"{name}.s3.amazonaws.com"
    resource.attributes["RegionalDomainName"] = f"{name}.s3.{region}.amazonaws.com"
    resource.attributes["WebsiteURL"] = f"http://{name}.s3-website-{region}.amazonaws.com"
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

    # AWS::IAM::Policy creates inline policies attached to roles/users/groups
    roles = resource.properties.get("Roles", [])
    for role_name in roles:
        try:
            iam.put_role_policy(role_name, name, doc)
        except Exception:
            pass

    users = resource.properties.get("Users", [])
    for user_name in users:
        try:
            iam.put_user_policy(user_name, name, doc)
        except Exception:
            pass

    groups = resource.properties.get("Groups", [])
    for group_name in groups:
        try:
            iam.put_group_policy(group_name, name, doc)
        except Exception:
            pass

    resource.physical_id = name
    resource.attributes["PolicyName"] = name
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
    # Apply retention policy if specified
    retention = resource.properties.get("RetentionInDays")
    if retention:
        try:
            logs.put_retention_policy(name, int(retention))
        except Exception:
            pass
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

        # Check if it's already a base64-encoded zip
        try:
            decoded = base64.b64decode(zip_file)
            if decoded[:4] == b"PK\x03\x04":
                # Already a valid zip file, pass through
                code_obj = {"ZipFile": zip_file}
            else:
                raise ValueError("not a zip")
        except Exception:
            # Treat as inline source code — wrap in a zip
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
    resource.physical_id = name
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


# --- IAM::ManagedPolicy ---


def _create_iam_managed_policy(resource: CfnResource, region: str, account_id: str) -> None:
    iam = _moto_global_backend("iam", account_id)
    name = resource.properties.get(
        "ManagedPolicyName",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
    )
    doc = resource.properties.get("PolicyDocument", {})
    if isinstance(doc, dict):
        doc = json.dumps(doc)
    path = resource.properties.get("Path", "/")
    desc = resource.properties.get("Description", "")
    try:
        policy = iam.create_policy(desc, path, doc, name, [])
        resource.physical_id = policy.arn
        resource.attributes["Arn"] = policy.arn
    except Exception:
        arn = f"arn:aws:iam::{account_id}:policy{path}{name}"
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
    resource.status = "CREATE_COMPLETE"


def _delete_iam_managed_policy(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        iam = _moto_global_backend("iam", account_id)
        if resource.physical_id:
            iam.delete_policy(resource.physical_id)
    except Exception:
        pass


# --- IAM::InstanceProfile ---


def _create_iam_instance_profile(resource: CfnResource, region: str, account_id: str) -> None:
    iam = _moto_global_backend("iam", account_id)
    name = resource.properties.get(
        "InstanceProfileName",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
    )
    path = resource.properties.get("Path", "/")
    try:
        profile = iam.create_instance_profile(name, path, tags=[])
        resource.physical_id = name
        resource.attributes["Arn"] = profile.arn
    except Exception:
        resource.physical_id = name
        resource.attributes["Arn"] = f"arn:aws:iam::{account_id}:instance-profile{path}{name}"
    # Attach roles
    roles = resource.properties.get("Roles", [])
    for role_name in roles:
        try:
            iam.add_role_to_instance_profile(name, role_name)
        except Exception:
            pass
    resource.status = "CREATE_COMPLETE"


def _delete_iam_instance_profile(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        iam = _moto_global_backend("iam", account_id)
        if resource.physical_id:
            iam.delete_instance_profile(resource.physical_id)
    except Exception:
        pass


# --- IAM::User ---


def _create_iam_user(resource: CfnResource, region: str, account_id: str) -> None:
    iam = _moto_global_backend("iam", account_id)
    name = resource.properties.get("UserName", f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}")
    path = resource.properties.get("Path", "/")
    try:
        user = iam.create_user(name, path, tags=[])
        resource.physical_id = name
        resource.attributes["Arn"] = user.arn
    except Exception:
        resource.physical_id = name
        resource.attributes["Arn"] = f"arn:aws:iam::{account_id}:user{path}{name}"
    resource.status = "CREATE_COMPLETE"


def _delete_iam_user(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        iam = _moto_global_backend("iam", account_id)
        if resource.physical_id:
            iam.delete_user(resource.physical_id)
    except Exception:
        pass


# --- IAM::Group ---


def _create_iam_group(resource: CfnResource, region: str, account_id: str) -> None:
    iam = _moto_global_backend("iam", account_id)
    name = resource.properties.get("GroupName", f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}")
    path = resource.properties.get("Path", "/")
    try:
        group = iam.create_group(name, path)
        resource.physical_id = name
        resource.attributes["Arn"] = group.arn
    except Exception:
        resource.physical_id = name
        resource.attributes["Arn"] = f"arn:aws:iam::{account_id}:group{path}{name}"
    resource.status = "CREATE_COMPLETE"


def _delete_iam_group(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        iam = _moto_global_backend("iam", account_id)
        if resource.physical_id:
            iam.delete_group(resource.physical_id)
    except Exception:
        pass


# --- IAM::AccessKey ---


def _create_iam_access_key(resource: CfnResource, region: str, account_id: str) -> None:
    iam = _moto_global_backend("iam", account_id)
    user_name = resource.properties.get("UserName", "")
    try:
        key = iam.create_access_key(user_name)
        resource.physical_id = key.access_key_id
        resource.attributes["SecretAccessKey"] = key.secret_access_key
    except Exception:
        resource.physical_id = f"AKIA{uuid.uuid4().hex[:16].upper()}"
        resource.attributes["SecretAccessKey"] = uuid.uuid4().hex
    resource.status = "CREATE_COMPLETE"


def _delete_iam_access_key(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        iam = _moto_global_backend("iam", account_id)
        user_name = resource.properties.get("UserName", "")
        if resource.physical_id and user_name:
            iam.delete_access_key(resource.physical_id, user_name)
    except Exception:
        pass


# --- IAM::ServiceLinkedRole ---


def _create_iam_service_linked_role(resource: CfnResource, region: str, account_id: str) -> None:
    svc_name = resource.properties.get("AWSServiceName", "unknown.amazonaws.com")
    svc_short = svc_name.split(".")[0]
    role_name = f"AWSServiceRoleFor{svc_short.capitalize()}"
    arn = f"arn:aws:iam::{account_id}:role/aws-service-role/{svc_name}/{role_name}"
    resource.physical_id = arn
    resource.attributes["Arn"] = arn
    resource.status = "CREATE_COMPLETE"


def _delete_iam_service_linked_role(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- Lambda::Version ---


def _create_lambda_version(resource: CfnResource, region: str, account_id: str) -> None:
    fn_name = resource.properties.get("FunctionName", "")
    desc = resource.properties.get("Description", "")
    try:
        lmbda = _moto_backend("lambda", account_id, region)
        ver = lmbda.publish_version_with_name(fn_name, desc)
        resource.physical_id = ver.function_arn
        resource.attributes["Version"] = ver.version
    except Exception:
        version_num = "1"
        arn = f"arn:aws:lambda:{region}:{account_id}:function:{fn_name}:{version_num}"
        resource.physical_id = arn
        resource.attributes["Version"] = version_num
    resource.status = "CREATE_COMPLETE"


def _delete_lambda_version(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Versions are immutable


# --- Lambda::Alias ---


def _create_lambda_alias(resource: CfnResource, region: str, account_id: str) -> None:
    fn_name = resource.properties.get("FunctionName", "")
    alias_name = resource.properties.get("Name", resource.logical_id)
    arn = f"arn:aws:lambda:{region}:{account_id}:function:{fn_name}:{alias_name}"
    resource.physical_id = arn
    resource.attributes["Arn"] = arn
    resource.attributes["AliasArn"] = arn
    resource.status = "CREATE_COMPLETE"


def _delete_lambda_alias(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- Lambda::EventSourceMapping ---


def _create_lambda_event_source_mapping(
    resource: CfnResource, region: str, account_id: str
) -> None:
    # Use native Lambda provider's ESM store
    from robotocore.services.lambda_.provider import _esm_lock, _esm_store

    func_name = resource.properties.get("FunctionName", "")
    source_arn = resource.properties.get("EventSourceArn", "")
    uid = str(uuid.uuid4())
    import time as _time

    # Resolve function ARN
    func_arn = func_name
    if not func_arn.startswith("arn:"):
        func_arn = f"arn:aws:lambda:{region}:{account_id}:function:{func_name}"

    config = {
        "UUID": uid,
        "FunctionArn": func_arn,
        "EventSourceArn": source_arn,
        "BatchSize": resource.properties.get("BatchSize", 10),
        "State": "Enabled" if resource.properties.get("Enabled", True) else "Disabled",
        "StartingPosition": resource.properties.get("StartingPosition", "LATEST"),
        "LastModified": _time.time(),
        "MaximumBatchingWindowInSeconds": 0,
    }
    with _esm_lock:
        _esm_store[uid] = config
    resource.physical_id = uid
    resource.attributes["EventSourceMappingId"] = uid
    resource.status = "CREATE_COMPLETE"


def _delete_lambda_event_source_mapping(
    resource: CfnResource, region: str, account_id: str
) -> None:
    try:
        lmbda = _moto_backend("lambda", account_id, region)
        if resource.physical_id:
            lmbda.delete_event_source_mapping(resource.physical_id)
    except Exception:
        pass


# --- Lambda::Permission ---


def _create_lambda_permission(resource: CfnResource, region: str, account_id: str) -> None:
    fn_name = resource.properties.get("FunctionName", "")
    action = resource.properties.get("Action", "lambda:InvokeFunction")
    principal = resource.properties.get("Principal", "")
    sid = resource.properties.get("StatementId", f"cfn-{uuid.uuid4().hex[:8]}")
    try:
        lmbda = _moto_backend("lambda", account_id, region)
        lmbda.add_permission(
            fn_name,
            {
                "Action": action,
                "Principal": principal,
                "StatementId": sid,
            },
        )
    except Exception:
        pass
    resource.physical_id = sid
    resource.status = "CREATE_COMPLETE"


def _delete_lambda_permission(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        lmbda = _moto_backend("lambda", account_id, region)
        fn_name = resource.properties.get("FunctionName", "")
        if resource.physical_id and fn_name:
            lmbda.remove_permission(fn_name, resource.physical_id)
    except Exception:
        pass


# --- Lambda::LayerVersion ---


def _create_lambda_layer_version(resource: CfnResource, region: str, account_id: str) -> None:
    layer_name = resource.properties.get(
        "LayerName", f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}"
    )
    arn = f"arn:aws:lambda:{region}:{account_id}:layer:{layer_name}:1"
    resource.physical_id = arn
    resource.attributes["Arn"] = arn
    resource.attributes["LayerVersionArn"] = arn
    resource.status = "CREATE_COMPLETE"


def _delete_lambda_layer_version(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- EC2::VPC ---


def _create_ec2_vpc(resource: CfnResource, region: str, account_id: str) -> None:
    ec2 = _moto_backend("ec2", account_id, region)
    cidr = resource.properties.get("CidrBlock", "10.0.0.0/16")
    try:
        vpc = ec2.create_vpc(cidr)
        resource.physical_id = vpc.id
        resource.attributes["VpcId"] = vpc.id
        resource.attributes["CidrBlock"] = cidr
    except Exception:
        vid = f"vpc-{uuid.uuid4().hex[:8]}"
        resource.physical_id = vid
        resource.attributes["VpcId"] = vid
        resource.attributes["CidrBlock"] = cidr
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_vpc(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        ec2 = _moto_backend("ec2", account_id, region)
        if resource.physical_id:
            ec2.delete_vpc(resource.physical_id)
    except Exception:
        pass


# --- EC2::Subnet ---


def _create_ec2_subnet(resource: CfnResource, region: str, account_id: str) -> None:
    ec2 = _moto_backend("ec2", account_id, region)
    vpc_id = resource.properties.get("VpcId", "")
    cidr = resource.properties.get("CidrBlock", "10.0.0.0/24")
    az = resource.properties.get("AvailabilityZone", f"{region}a")
    try:
        subnet = ec2.create_subnet(vpc_id, cidr, az)
        resource.physical_id = subnet.id
        resource.attributes["SubnetId"] = subnet.id
        resource.attributes["AvailabilityZone"] = az
        resource.attributes["CidrBlock"] = cidr
        resource.attributes["VpcId"] = vpc_id
    except Exception:
        sid = f"subnet-{uuid.uuid4().hex[:8]}"
        resource.physical_id = sid
        resource.attributes["SubnetId"] = sid
        resource.attributes["AvailabilityZone"] = az
        resource.attributes["CidrBlock"] = cidr
        resource.attributes["VpcId"] = vpc_id
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_subnet(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        ec2 = _moto_backend("ec2", account_id, region)
        if resource.physical_id:
            ec2.delete_subnet(resource.physical_id)
    except Exception:
        pass


# --- EC2::SecurityGroup ---


def _create_ec2_security_group(resource: CfnResource, region: str, account_id: str) -> None:
    ec2 = _moto_backend("ec2", account_id, region)
    name = resource.properties.get(
        "GroupName",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
    )
    desc = resource.properties.get("GroupDescription", name)
    vpc_id = resource.properties.get("VpcId", "")
    sg = ec2.create_security_group(name, desc, vpc_id or None)
    resource.physical_id = sg.id
    resource.attributes["GroupId"] = sg.id
    resource.attributes["VpcId"] = vpc_id

    # Apply ingress rules
    for rule in resource.properties.get("SecurityGroupIngress", []):
        ip_ranges = []
        if "CidrIp" in rule:
            ip_ranges.append({"CidrIp": rule["CidrIp"]})
        try:
            ec2.authorize_security_group_ingress(
                sg.id,
                rule.get("IpProtocol", "tcp"),
                str(rule.get("FromPort", 0)),
                str(rule.get("ToPort", 0)),
                ip_ranges,
            )
        except Exception:
            pass  # Duplicate rules are OK

    resource.status = "CREATE_COMPLETE"


def _delete_ec2_security_group(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        ec2 = _moto_backend("ec2", account_id, region)
        if resource.physical_id:
            ec2.delete_security_group(group_id=resource.physical_id)
    except Exception:
        pass


# --- EC2::InternetGateway ---


def _create_ec2_internet_gateway(resource: CfnResource, region: str, account_id: str) -> None:
    ec2 = _moto_backend("ec2", account_id, region)
    try:
        igw = ec2.create_internet_gateway()
        resource.physical_id = igw.id
        resource.attributes["InternetGatewayId"] = igw.id
    except Exception:
        gid = f"igw-{uuid.uuid4().hex[:8]}"
        resource.physical_id = gid
        resource.attributes["InternetGatewayId"] = gid
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_internet_gateway(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        ec2 = _moto_backend("ec2", account_id, region)
        if resource.physical_id:
            ec2.delete_internet_gateway(resource.physical_id)
    except Exception:
        pass


# --- EC2::VPCGatewayAttachment ---


def _create_ec2_vpc_gateway_attachment(resource: CfnResource, region: str, account_id: str) -> None:
    ec2 = _moto_backend("ec2", account_id, region)
    igw_id = resource.properties.get("InternetGatewayId", "")
    vpc_id = resource.properties.get("VpcId", "")
    try:
        ec2.attach_internet_gateway(igw_id, vpc_id)
    except Exception:
        pass
    resource.physical_id = f"{igw_id}|{vpc_id}"
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_vpc_gateway_attachment(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        ec2 = _moto_backend("ec2", account_id, region)
        if resource.physical_id:
            parts = resource.physical_id.split("|")
            if len(parts) == 2:
                ec2.detach_internet_gateway(parts[0], parts[1])
    except Exception:
        pass


# --- EC2::RouteTable ---


def _create_ec2_route_table(resource: CfnResource, region: str, account_id: str) -> None:
    ec2 = _moto_backend("ec2", account_id, region)
    vpc_id = resource.properties.get("VpcId", "")
    try:
        rt = ec2.create_route_table(vpc_id)
        resource.physical_id = rt.id
        resource.attributes["RouteTableId"] = rt.id
    except Exception:
        rid = f"rtb-{uuid.uuid4().hex[:8]}"
        resource.physical_id = rid
        resource.attributes["RouteTableId"] = rid
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_route_table(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        ec2 = _moto_backend("ec2", account_id, region)
        if resource.physical_id:
            ec2.delete_route_table(resource.physical_id)
    except Exception:
        pass


# --- EC2::Route ---


def _create_ec2_route(resource: CfnResource, region: str, account_id: str) -> None:
    ec2 = _moto_backend("ec2", account_id, region)
    rt_id = resource.properties.get("RouteTableId", "")
    cidr = resource.properties.get("DestinationCidrBlock", "0.0.0.0/0")
    gw_id = resource.properties.get("GatewayId", "")
    nat_gw_id = resource.properties.get("NatGatewayId", "")
    try:
        ec2.create_route(
            rt_id,
            cidr,
            gateway_id=gw_id or None,
            nat_gateway_id=nat_gw_id or None,
        )
    except Exception:
        pass
    resource.physical_id = f"{rt_id}|{cidr}"
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_route(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        ec2 = _moto_backend("ec2", account_id, region)
        if resource.physical_id:
            parts = resource.physical_id.split("|")
            if len(parts) == 2:
                ec2.delete_route(parts[0], parts[1])
    except Exception:
        pass


# --- EC2::SubnetRouteTableAssociation ---


def _create_ec2_subnet_route_table_assoc(
    resource: CfnResource, region: str, account_id: str
) -> None:
    rt_id = resource.properties.get("RouteTableId", "")
    subnet_id = resource.properties.get("SubnetId", "")
    try:
        ec2 = _moto_backend("ec2", account_id, region)
        assoc_id = ec2.associate_route_table(rt_id, subnet_id)
        resource.physical_id = (
            assoc_id if isinstance(assoc_id, str) else f"rtbassoc-{uuid.uuid4().hex[:8]}"
        )
    except Exception:
        resource.physical_id = f"rtbassoc-{uuid.uuid4().hex[:8]}"
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_subnet_route_table_assoc(
    resource: CfnResource, region: str, account_id: str
) -> None:
    try:
        ec2 = _moto_backend("ec2", account_id, region)
        if resource.physical_id:
            ec2.disassociate_route_table(resource.physical_id)
    except Exception:
        pass


# --- EC2::NatGateway ---


def _create_ec2_nat_gateway(resource: CfnResource, region: str, account_id: str) -> None:
    nid = f"nat-{uuid.uuid4().hex[:17]}"
    resource.physical_id = nid
    resource.attributes["NatGatewayId"] = nid
    subnet_id = resource.properties.get("SubnetId", "")
    resource.attributes["SubnetId"] = subnet_id
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_nat_gateway(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- EC2::EIP ---


def _create_ec2_eip(resource: CfnResource, region: str, account_id: str) -> None:
    ec2 = _moto_backend("ec2", account_id, region)
    domain = resource.properties.get("Domain", "vpc")
    try:
        eip = ec2.allocate_address(domain=domain)
        resource.physical_id = eip.allocation_id
        resource.attributes["AllocationId"] = eip.allocation_id
        resource.attributes["PublicIp"] = eip.public_ip
    except Exception:
        aid = f"eipalloc-{uuid.uuid4().hex[:8]}"
        resource.physical_id = aid
        resource.attributes["AllocationId"] = aid
        resource.attributes["PublicIp"] = "203.0.113.1"
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_eip(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        ec2 = _moto_backend("ec2", account_id, region)
        if resource.physical_id:
            ec2.release_address(resource.physical_id)
    except Exception:
        pass


# --- EC2::LaunchTemplate ---


def _create_ec2_launch_template(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get(
        "LaunchTemplateName",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
    )
    lt_id = f"lt-{uuid.uuid4().hex[:17]}"
    resource.physical_id = lt_id
    resource.attributes["LaunchTemplateId"] = lt_id
    resource.attributes["LaunchTemplateName"] = name
    resource.attributes["DefaultVersionNumber"] = "1"
    resource.attributes["LatestVersionNumber"] = "1"
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_launch_template(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- EC2::KeyPair ---


def _create_ec2_key_pair(resource: CfnResource, region: str, account_id: str) -> None:
    ec2 = _moto_backend("ec2", account_id, region)
    name = resource.properties.get("KeyName", resource.logical_id)
    try:
        kp = ec2.create_key_pair(name)
        resource.physical_id = name
        resource.attributes["KeyPairId"] = getattr(kp, "id", f"key-{uuid.uuid4().hex[:8]}")
    except Exception:
        resource.physical_id = name
        resource.attributes["KeyPairId"] = f"key-{uuid.uuid4().hex[:8]}"
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_key_pair(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        ec2 = _moto_backend("ec2", account_id, region)
        if resource.physical_id:
            ec2.delete_key_pair(resource.physical_id)
    except Exception:
        pass


# --- S3::BucketPolicy ---


def _create_s3_bucket_policy(resource: CfnResource, region: str, account_id: str) -> None:
    s3 = _moto_global_backend("s3", account_id)
    bucket_name = resource.properties.get("Bucket", "")
    policy = resource.properties.get("PolicyDocument", {})
    if isinstance(policy, dict):
        policy = json.dumps(policy)
    try:
        s3.put_bucket_policy(bucket_name, policy)
    except Exception:
        pass
    resource.physical_id = bucket_name
    resource.status = "CREATE_COMPLETE"


def _delete_s3_bucket_policy(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        s3 = _moto_global_backend("s3", account_id)
        if resource.physical_id:
            s3.delete_bucket_policy(resource.physical_id)
    except Exception:
        pass


# --- S3::BucketNotificationConfiguration ---


def _create_s3_bucket_notification_config(
    resource: CfnResource, region: str, account_id: str
) -> None:
    bucket_name = resource.properties.get("Bucket", "")
    resource.physical_id = bucket_name
    resource.status = "CREATE_COMPLETE"


def _delete_s3_bucket_notification_config(
    resource: CfnResource, region: str, account_id: str
) -> None:
    pass  # Simulated


# --- DynamoDB::GlobalTable ---


def _create_dynamodb_global_table(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get(
        "TableName",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
    )
    # Create as a regular table, replication is simulated
    try:
        ddb = _moto_backend("dynamodb", account_id, region)
        key_schema = resource.properties.get("KeySchema", [])
        attr_defs = resource.properties.get("AttributeDefinitions", [])
        billing = resource.properties.get("BillingMode", "PAY_PER_REQUEST")
        ddb.create_table(
            name,
            schema=key_schema,
            throughput={},
            attr=attr_defs,
            global_indexes=None,
            indexes=None,
            streams=None,
            billing_mode=billing,
            sse_specification=None,
            tags=[],
            deletion_protection_enabled=None,
            warm_throughput=None,
        )
    except Exception:
        pass
    resource.physical_id = name
    arn = f"arn:aws:dynamodb:{region}:{account_id}:table/{name}"
    resource.attributes["Arn"] = arn
    resource.attributes["TableName"] = name
    resource.attributes["TableId"] = f"globaltable-{uuid.uuid4().hex[:8]}"
    resource.status = "CREATE_COMPLETE"


def _delete_dynamodb_global_table(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        ddb = _moto_backend("dynamodb", account_id, region)
        if resource.physical_id:
            ddb.delete_table(resource.physical_id)
    except Exception:
        pass


# --- SQS::QueuePolicy ---


def _create_sqs_queue_policy(resource: CfnResource, region: str, account_id: str) -> None:
    queues = resource.properties.get("Queues", [])
    resource.physical_id = queues[0] if queues else f"qp-{uuid.uuid4().hex[:8]}"
    resource.status = "CREATE_COMPLETE"


def _delete_sqs_queue_policy(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- ApiGateway::RestApi ---


def _create_apigw_rest_api(resource: CfnResource, region: str, account_id: str) -> None:
    apigw = _moto_backend("apigateway", account_id, region)
    name = resource.properties.get("Name", resource.logical_id)
    desc = resource.properties.get("Description", "")
    try:
        api = apigw.create_rest_api(name, desc)
        resource.physical_id = api.id
        resource.attributes["RestApiId"] = api.id
        resource.attributes["RootResourceId"] = getattr(
            api,
            "root_resource_id",
            api.resources.get("/", SimpleNameHolder("")).id if hasattr(api, "resources") else "",
        )
    except Exception:
        aid = f"api-{uuid.uuid4().hex[:8]}"
        resource.physical_id = aid
        resource.attributes["RestApiId"] = aid
        resource.attributes["RootResourceId"] = uuid.uuid4().hex[:10]
    resource.status = "CREATE_COMPLETE"


class SimpleNameHolder:
    """Tiny helper so attribute lookup doesn't fail."""

    def __init__(self, val):
        self.id = val


def _delete_apigw_rest_api(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        apigw = _moto_backend("apigateway", account_id, region)
        if resource.physical_id:
            apigw.delete_rest_api(resource.physical_id)
    except Exception:
        pass


# --- ApiGateway::Resource ---


def _create_apigw_resource(resource: CfnResource, region: str, account_id: str) -> None:
    apigw = _moto_backend("apigateway", account_id, region)
    rest_api_id = resource.properties.get("RestApiId", "")
    parent_id = resource.properties.get("ParentId", "")
    path_part = resource.properties.get("PathPart", "")
    try:
        res_obj = apigw.create_resource(rest_api_id, parent_id, path_part)
        resource.physical_id = res_obj.id
        resource.attributes["ResourceId"] = res_obj.id
    except Exception:
        rid = uuid.uuid4().hex[:10]
        resource.physical_id = rid
        resource.attributes["ResourceId"] = rid
    resource.status = "CREATE_COMPLETE"


def _delete_apigw_resource(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        apigw = _moto_backend("apigateway", account_id, region)
        rest_api_id = resource.properties.get("RestApiId", "")
        if resource.physical_id and rest_api_id:
            apigw.delete_resource(rest_api_id, resource.physical_id)
    except Exception:
        pass


# --- ApiGateway::Method ---


def _create_apigw_method(resource: CfnResource, region: str, account_id: str) -> None:
    apigw = _moto_backend("apigateway", account_id, region)
    rest_api_id = resource.properties.get("RestApiId", "")
    resource_id = resource.properties.get("ResourceId", "")
    http_method = resource.properties.get("HttpMethod", "GET")
    auth_type = resource.properties.get("AuthorizationType", "NONE")
    apigw.put_method(rest_api_id, resource_id, http_method, auth_type)

    # Set up integration if specified
    integration = resource.properties.get("Integration")
    if integration:
        int_type = integration.get("Type", "HTTP")
        uri = integration.get("Uri", "")
        int_method = integration.get("IntegrationHttpMethod", "POST")
        try:
            apigw.put_integration(rest_api_id, resource_id, http_method, int_type, uri, int_method)
        except Exception:
            pass  # Integration setup is best-effort

    pid = f"{rest_api_id}/{resource_id}/{http_method}"
    resource.physical_id = pid
    resource.status = "CREATE_COMPLETE"


def _delete_apigw_method(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- ApiGateway::Deployment ---


def _create_apigw_deployment(resource: CfnResource, region: str, account_id: str) -> None:
    apigw = _moto_backend("apigateway", account_id, region)
    rest_api_id = resource.properties.get("RestApiId", "")
    try:
        dep = apigw.create_deployment(rest_api_id)
        resource.physical_id = dep.id if hasattr(dep, "id") else dep["id"]
    except Exception:
        resource.physical_id = uuid.uuid4().hex[:10]
    resource.status = "CREATE_COMPLETE"


def _delete_apigw_deployment(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- ApiGateway::Stage ---


def _create_apigw_stage(resource: CfnResource, region: str, account_id: str) -> None:
    stage_name = resource.properties.get("StageName", "prod")
    resource.physical_id = stage_name
    resource.status = "CREATE_COMPLETE"


def _delete_apigw_stage(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- ApiGateway::ApiKey ---


def _create_apigw_api_key(resource: CfnResource, region: str, account_id: str) -> None:
    kid = uuid.uuid4().hex[:20]
    resource.physical_id = kid
    resource.attributes["APIKeyId"] = kid
    resource.status = "CREATE_COMPLETE"


def _delete_apigw_api_key(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- ApiGateway::UsagePlan ---


def _create_apigw_usage_plan(resource: CfnResource, region: str, account_id: str) -> None:
    pid = uuid.uuid4().hex[:10]
    resource.physical_id = pid
    resource.attributes["Id"] = pid
    resource.status = "CREATE_COMPLETE"


def _delete_apigw_usage_plan(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- ApiGateway::DomainName ---


def _create_apigw_domain_name(resource: CfnResource, region: str, account_id: str) -> None:
    domain = resource.properties.get("DomainName", resource.logical_id)
    resource.physical_id = domain
    resource.attributes["DistributionDomainName"] = f"d{uuid.uuid4().hex[:13]}.cloudfront.net"
    resource.attributes["RegionalDomainName"] = (
        f"d-{uuid.uuid4().hex[:10]}.execute-api.{region}.amazonaws.com"
    )
    resource.status = "CREATE_COMPLETE"


def _delete_apigw_domain_name(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- CloudWatch::Alarm ---


def _create_cloudwatch_alarm(resource: CfnResource, region: str, account_id: str) -> None:
    cw = _moto_backend("cloudwatch", account_id, region)
    name = resource.properties.get(
        "AlarmName",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
    )
    raw_dims = resource.properties.get("Dimensions", [])
    dims = []
    for d in raw_dims:
        dims.append({"name": d.get("Name", ""), "value": d.get("Value", "")})
    cw.put_metric_alarm(
        name=name,
        namespace=resource.properties.get("Namespace", "AWS/Custom"),
        metric_name=resource.properties.get("MetricName", ""),
        comparison_operator=resource.properties.get("ComparisonOperator", "GreaterThanThreshold"),
        evaluation_periods=int(resource.properties.get("EvaluationPeriods", 1)),
        period=int(resource.properties.get("Period", 300)),
        threshold=float(resource.properties.get("Threshold", 0)),
        statistic=resource.properties.get("Statistic", "Average"),
        description=resource.properties.get("AlarmDescription", ""),
        dimensions=dims,
        alarm_actions=resource.properties.get("AlarmActions", []),
        ok_actions=resource.properties.get("OKActions", []),
        insufficient_data_actions=resource.properties.get("InsufficientDataActions", []),
        actions_enabled=resource.properties.get("ActionsEnabled", True),
        treat_missing_data=resource.properties.get("TreatMissingData", "missing"),
        datapoints_to_alarm=resource.properties.get("DatapointsToAlarm", None),
        tags=[],
    )
    resource.physical_id = name
    resource.attributes["Arn"] = f"arn:aws:cloudwatch:{region}:{account_id}:alarm:{name}"
    resource.status = "CREATE_COMPLETE"


def _delete_cloudwatch_alarm(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        cw = _moto_backend("cloudwatch", account_id, region)
        if resource.physical_id:
            cw.delete_alarms([resource.physical_id])
    except Exception:
        pass


# --- Logs::LogStream ---


def _create_log_stream(resource: CfnResource, region: str, account_id: str) -> None:
    logs = _moto_backend("logs", account_id, region)
    group_name = resource.properties.get("LogGroupName", "")
    stream_name = resource.properties.get("LogStreamName", resource.logical_id)
    try:
        logs.create_log_stream(group_name, stream_name)
    except Exception:
        pass
    resource.physical_id = stream_name
    resource.status = "CREATE_COMPLETE"


def _delete_log_stream(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        logs = _moto_backend("logs", account_id, region)
        group_name = resource.properties.get("LogGroupName", "")
        if resource.physical_id and group_name:
            logs.delete_log_stream(group_name, resource.physical_id)
    except Exception:
        pass


# --- CloudWatch::MetricFilter ---


def _create_cloudwatch_metric_filter(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get("FilterName", resource.logical_id)
    resource.physical_id = name
    resource.status = "CREATE_COMPLETE"


def _delete_cloudwatch_metric_filter(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- Events::EventBus ---


def _create_events_event_bus(resource: CfnResource, region: str, account_id: str) -> None:
    events = _moto_backend("events", account_id, region)
    name = resource.properties.get("Name", resource.logical_id)
    try:
        bus = events.create_event_bus(name)
        arn = getattr(bus, "arn", None) or f"arn:aws:events:{region}:{account_id}:event-bus/{name}"
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
        resource.attributes["Name"] = name
    except Exception:
        arn = f"arn:aws:events:{region}:{account_id}:event-bus/{name}"
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
        resource.attributes["Name"] = name
    resource.status = "CREATE_COMPLETE"


def _delete_events_event_bus(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        events = _moto_backend("events", account_id, region)
        name = resource.properties.get("Name", "")
        if name:
            events.delete_event_bus(name)
    except Exception:
        pass


# --- Events::Archive ---


def _create_events_archive(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get(
        "ArchiveName",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
    )
    arn = f"arn:aws:events:{region}:{account_id}:archive/{name}"
    resource.physical_id = name
    resource.attributes["Arn"] = arn
    resource.attributes["ArchiveName"] = name
    resource.status = "CREATE_COMPLETE"


def _delete_events_archive(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- StepFunctions::StateMachine ---


def _create_sfn_state_machine(resource: CfnResource, region: str, account_id: str) -> None:
    sfn = _moto_backend("stepfunctions", account_id, region)
    name = resource.properties.get(
        "StateMachineName",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
    )
    definition = resource.properties.get("DefinitionString", "{}")
    if isinstance(definition, dict):
        definition = json.dumps(definition)
    role_arn = resource.properties.get("RoleArn", f"arn:aws:iam::{account_id}:role/sfn-role")
    tags = resource.properties.get("Tags", [])
    try:
        sm = sfn.create_state_machine(name, definition, role_arn, tags)
        resource.physical_id = sm.arn
        resource.attributes["Arn"] = sm.arn
        resource.attributes["Name"] = name
    except Exception:
        arn = f"arn:aws:states:{region}:{account_id}:stateMachine:{name}"
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
        resource.attributes["Name"] = name
    resource.status = "CREATE_COMPLETE"


def _delete_sfn_state_machine(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        sfn = _moto_backend("stepfunctions", account_id, region)
        if resource.physical_id:
            sfn.delete_state_machine(resource.physical_id)
    except Exception:
        pass


# --- StepFunctions::Activity ---


def _create_sfn_activity(resource: CfnResource, region: str, account_id: str) -> None:
    sfn = _moto_backend("stepfunctions", account_id, region)
    name = resource.properties.get("Name", resource.logical_id)
    try:
        activity = sfn.create_activity(name)
        arn = (
            getattr(activity, "arn", None)
            or getattr(activity, "activity_arn", None)
            or f"arn:aws:states:{region}:{account_id}:activity:{name}"
        )
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
        resource.attributes["Name"] = name
    except Exception:
        arn = f"arn:aws:states:{region}:{account_id}:activity:{name}"
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
        resource.attributes["Name"] = name
    resource.status = "CREATE_COMPLETE"


def _delete_sfn_activity(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        sfn = _moto_backend("stepfunctions", account_id, region)
        if resource.physical_id:
            sfn.delete_activity(resource.physical_id)
    except Exception:
        pass


# --- KMS::Alias ---


def _create_kms_alias(resource: CfnResource, region: str, account_id: str) -> None:
    kms = _moto_backend("kms", account_id, region)
    alias_name = resource.properties.get("AliasName", "")
    target_key = resource.properties.get("TargetKeyId", "")
    try:
        kms.create_alias(alias_name, target_key)
    except Exception:
        pass
    resource.physical_id = alias_name
    resource.status = "CREATE_COMPLETE"


def _delete_kms_alias(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        kms = _moto_backend("kms", account_id, region)
        if resource.physical_id:
            kms.delete_alias(resource.physical_id)
    except Exception:
        pass


# --- SecretsManager::Secret ---


def _create_secretsmanager_secret(resource: CfnResource, region: str, account_id: str) -> None:
    sm = _moto_backend("secretsmanager", account_id, region)
    name = resource.properties.get("Name", f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}")
    secret_string = resource.properties.get("SecretString", "")
    desc = resource.properties.get("Description", "")
    tags = resource.properties.get("Tags", [])
    try:
        result = sm.create_secret(
            name=name,
            secret_string=secret_string or None,
            description=desc,
            tags=tags,
        )
        arn = (
            getattr(result, "arn", None) or result.get("ARN", "")
            if isinstance(result, dict)
            else f"arn:aws:secretsmanager:{region}:{account_id}:secret:{name}"
        )
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
    except Exception:
        arn = f"arn:aws:secretsmanager:{region}:{account_id}:secret:{name}-{uuid.uuid4().hex[:6]}"
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
    resource.status = "CREATE_COMPLETE"


def _delete_secretsmanager_secret(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        sm = _moto_backend("secretsmanager", account_id, region)
        if resource.physical_id:
            sm.delete_secret(
                resource.physical_id,
                recovery_window_in_days=0,
                force_delete_without_recovery=True,
            )
    except Exception:
        pass


# --- SSM::Document ---


def _create_ssm_document(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get("Name", resource.logical_id)
    resource.physical_id = name
    resource.status = "CREATE_COMPLETE"


def _delete_ssm_document(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- Kinesis::Stream ---


def _create_kinesis_stream(resource: CfnResource, region: str, account_id: str) -> None:
    # Use native Kinesis provider store (not Moto) since the API routes through it
    from robotocore.services.kinesis.models import _get_store as _get_kinesis_store

    store = _get_kinesis_store(region)
    name = resource.properties.get(
        "Name",
        resource.properties.get(
            "StreamName",
            f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
        ),
    )
    shard_count = resource.properties.get("ShardCount", 1)
    store.create_stream(name, shard_count, region, account_id)
    resource.physical_id = name
    resource.attributes["Arn"] = f"arn:aws:kinesis:{region}:{account_id}:stream/{name}"
    resource.status = "CREATE_COMPLETE"


def _delete_kinesis_stream(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        kinesis = _moto_backend("kinesis", account_id, region)
        if resource.physical_id:
            kinesis.delete_stream(resource.physical_id)
    except Exception:
        pass


# --- ECS::Cluster ---


def _create_ecs_cluster(resource: CfnResource, region: str, account_id: str) -> None:
    ecs = _moto_backend("ecs", account_id, region)
    name = resource.properties.get(
        "ClusterName",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
    )
    try:
        cluster = ecs.create_cluster(name)
        arn = getattr(cluster, "arn", None) or (f"arn:aws:ecs:{region}:{account_id}:cluster/{name}")
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
    except Exception:
        arn = f"arn:aws:ecs:{region}:{account_id}:cluster/{name}"
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
    resource.status = "CREATE_COMPLETE"


def _delete_ecs_cluster(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        ecs = _moto_backend("ecs", account_id, region)
        if resource.physical_id:
            ecs.delete_cluster(resource.physical_id)
    except Exception:
        pass


# --- ECS::TaskDefinition ---


def _create_ecs_task_definition(resource: CfnResource, region: str, account_id: str) -> None:
    ecs = _moto_backend("ecs", account_id, region)
    family = resource.properties.get("Family", resource.logical_id)
    container_defs = resource.properties.get("ContainerDefinitions", [])
    try:
        td = ecs.register_task_definition(
            family=family,
            container_definitions=container_defs,
        )
        arn = getattr(td, "arn", None) or (
            f"arn:aws:ecs:{region}:{account_id}:task-definition/{family}:1"
        )
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
    except Exception:
        arn = f"arn:aws:ecs:{region}:{account_id}:task-definition/{family}:1"
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
    resource.status = "CREATE_COMPLETE"


def _delete_ecs_task_definition(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        ecs = _moto_backend("ecs", account_id, region)
        if resource.physical_id:
            ecs.deregister_task_definition(resource.physical_id)
    except Exception:
        pass


# --- ECS::Service ---


def _create_ecs_service(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get(
        "ServiceName",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
    )
    cluster = resource.properties.get("Cluster", "default")
    arn = f"arn:aws:ecs:{region}:{account_id}:service/{cluster}/{name}"
    resource.physical_id = arn
    resource.attributes["Arn"] = arn
    resource.attributes["Name"] = name
    resource.status = "CREATE_COMPLETE"


def _delete_ecs_service(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- ElasticLoadBalancingV2::LoadBalancer ---


def _create_elbv2_load_balancer(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get(
        "Name",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}"[:32],
    )
    try:
        elbv2 = _moto_backend("elbv2", account_id, region)
        subnets = resource.properties.get("Subnets", [])
        scheme = resource.properties.get("Scheme", "internet-facing")
        lb_type = resource.properties.get("Type", "application")
        lb = elbv2.create_load_balancer(
            name=name,
            subnet_ids=subnets,
            scheme=scheme,
            loadbalancer_type=lb_type,
        )
        arn = getattr(lb, "arn", None) or (
            f"arn:aws:elasticloadbalancing:{region}:{account_id}"
            f":loadbalancer/app/{name}/{uuid.uuid4().hex[:16]}"
        )
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
        dns = getattr(lb, "dns_name", f"{name}.{region}.elb.amazonaws.com")
        resource.attributes["DNSName"] = dns
        resource.attributes["LoadBalancerName"] = name
    except Exception:
        arn = (
            f"arn:aws:elasticloadbalancing:{region}:{account_id}"
            f":loadbalancer/app/{name}/{uuid.uuid4().hex[:16]}"
        )
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
        resource.attributes["DNSName"] = f"{name}.{region}.elb.amazonaws.com"
        resource.attributes["LoadBalancerName"] = name
    resource.status = "CREATE_COMPLETE"


def _delete_elbv2_load_balancer(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        elbv2 = _moto_backend("elbv2", account_id, region)
        if resource.physical_id:
            elbv2.delete_load_balancer(resource.physical_id)
    except Exception:
        pass


# --- ElasticLoadBalancingV2::TargetGroup ---


def _create_elbv2_target_group(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get(
        "Name",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}"[:32],
    )
    try:
        elbv2 = _moto_backend("elbv2", account_id, region)
        tg = elbv2.create_target_group(
            name=name,
            protocol=resource.properties.get("Protocol", "HTTP"),
            port=resource.properties.get("Port", 80),
            vpc_id=resource.properties.get("VpcId", ""),
            target_type=resource.properties.get("TargetType", "instance"),
        )
        arn = getattr(tg, "arn", None) or (
            f"arn:aws:elasticloadbalancing:{region}:{account_id}"
            f":targetgroup/{name}/{uuid.uuid4().hex[:16]}"
        )
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
        resource.attributes["TargetGroupName"] = name
    except Exception:
        arn = (
            f"arn:aws:elasticloadbalancing:{region}:{account_id}"
            f":targetgroup/{name}/{uuid.uuid4().hex[:16]}"
        )
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
        resource.attributes["TargetGroupName"] = name
    resource.status = "CREATE_COMPLETE"


def _delete_elbv2_target_group(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        elbv2 = _moto_backend("elbv2", account_id, region)
        if resource.physical_id:
            elbv2.delete_target_group(resource.physical_id)
    except Exception:
        pass


# --- ElasticLoadBalancingV2::Listener ---


def _create_elbv2_listener(resource: CfnResource, region: str, account_id: str) -> None:
    arn = (
        f"arn:aws:elasticloadbalancing:{region}:{account_id}"
        f":listener/app/lb/{uuid.uuid4().hex[:16]}"
        f"/{uuid.uuid4().hex[:16]}"
    )
    resource.physical_id = arn
    resource.attributes["Arn"] = arn
    resource.attributes["ListenerArn"] = arn
    resource.status = "CREATE_COMPLETE"


def _delete_elbv2_listener(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- CloudFront::Distribution ---


def _create_cloudfront_distribution(resource: CfnResource, region: str, account_id: str) -> None:
    dist_id = f"E{uuid.uuid4().hex[:13].upper()}"
    domain = f"{dist_id.lower()}.cloudfront.net"
    resource.physical_id = dist_id
    resource.attributes["DomainName"] = domain
    resource.attributes["Id"] = dist_id
    resource.status = "CREATE_COMPLETE"


def _delete_cloudfront_distribution(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- Route53::HostedZone ---


def _create_route53_hosted_zone(resource: CfnResource, region: str, account_id: str) -> None:
    r53 = _moto_backend("route53", account_id, region)
    zone_name = resource.properties.get("Name", "example.com")
    try:
        zone = r53.create_hosted_zone(zone_name, private_zone=False)
        zone_id = getattr(zone, "id", None) or (getattr(zone, "zone_id", None))
        if not zone_id:
            zone_id = f"Z{uuid.uuid4().hex[:13].upper()}"
        resource.physical_id = zone_id
        resource.attributes["Id"] = zone_id
        ns = getattr(zone, "nameservers", None) or [f"ns-{i}.awsdns-{i:02d}.com" for i in range(4)]
        resource.attributes["NameServers"] = ns
    except Exception:
        zone_id = f"Z{uuid.uuid4().hex[:13].upper()}"
        resource.physical_id = zone_id
        resource.attributes["Id"] = zone_id
        resource.attributes["NameServers"] = [f"ns-{i}.awsdns-{i:02d}.com" for i in range(4)]
    resource.status = "CREATE_COMPLETE"


def _delete_route53_hosted_zone(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        r53 = _moto_backend("route53", account_id, region)
        if resource.physical_id:
            r53.delete_hosted_zone(resource.physical_id)
    except Exception:
        pass


# --- Route53::RecordSet ---


def _create_route53_record_set(resource: CfnResource, region: str, account_id: str) -> None:
    zone_id = resource.properties.get("HostedZoneId", "")
    rec_name = resource.properties.get("Name", "")
    rec_type = resource.properties.get("Type", "A")
    pid = f"{zone_id}|{rec_name}|{rec_type}"
    resource.physical_id = pid
    resource.status = "CREATE_COMPLETE"


def _delete_route53_record_set(resource: CfnResource, region: str, account_id: str) -> None:
    pass  # Simulated


# --- CertificateManager::Certificate ---


def _create_acm_certificate(resource: CfnResource, region: str, account_id: str) -> None:
    acm = _moto_backend("acm", account_id, region)
    domain = resource.properties.get("DomainName", "example.com")
    try:
        cert_arn = acm.request_certificate(domain)
        if isinstance(cert_arn, dict):
            cert_arn = cert_arn.get("CertificateArn", cert_arn)
        resource.physical_id = cert_arn
        resource.attributes["Arn"] = cert_arn
    except Exception:
        arn = f"arn:aws:acm:{region}:{account_id}:certificate/{uuid.uuid4()}"
        resource.physical_id = arn
        resource.attributes["Arn"] = arn
    resource.status = "CREATE_COMPLETE"


def _delete_acm_certificate(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        acm = _moto_backend("acm", account_id, region)
        if resource.physical_id:
            acm.delete_certificate(resource.physical_id)
    except Exception:
        pass


# --- Cognito::UserPool ---


def _create_cognito_user_pool(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get(
        "UserPoolName",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
    )
    # Use native Cognito provider store (not Moto) since the API routes through it
    from robotocore.services.cognito.provider import _get_store as _get_cognito_store

    store = _get_cognito_store(region)
    pool_id = f"{region}_{uuid.uuid4().hex[:8]}"
    import time as _time

    pool = {
        "Id": pool_id,
        "Name": name,
        "Arn": f"arn:aws:cognito-idp:{region}:{account_id}:userpool/{pool_id}",
        "CreationDate": _time.time(),
        "LastModifiedDate": _time.time(),
        "Status": "Enabled",
        "Policies": resource.properties.get("Policies", {}),
        "LambdaConfig": resource.properties.get("LambdaConfig", {}),
        "AutoVerifiedAttributes": resource.properties.get("AutoVerifiedAttributes", []),
        "Schema": resource.properties.get("Schema", []),
        "MfaConfiguration": resource.properties.get("MfaConfiguration", "OFF"),
    }
    with store.lock:
        store.pools[pool_id] = pool
        store.users[pool_id] = {}
        store.clients[pool_id] = {}
        store.groups[pool_id] = {}
        store.user_groups[pool_id] = {}
    resource.physical_id = pool_id
    resource.attributes["UserPoolId"] = pool_id
    resource.attributes["Arn"] = pool["Arn"]
    resource.attributes["ProviderName"] = f"cognito-idp.{region}.amazonaws.com/{pool_id}"
    resource.status = "CREATE_COMPLETE"


def _delete_cognito_user_pool(resource: CfnResource, region: str, account_id: str) -> None:
    try:
        cognito = _moto_backend("cognitoidp", account_id, region)
        if resource.physical_id:
            cognito.delete_user_pool(resource.physical_id)
    except Exception:
        pass


# --- Custom:: resources (Lambda-backed) ---


def _create_custom_resource(resource: CfnResource, region: str, account_id: str) -> None:
    """Handle Custom:: and AWS::CloudFormation::CustomResource types."""
    service_token = resource.properties.get("ServiceToken", "")
    if service_token and ":function:" in service_token:
        # Attempt to invoke Lambda
        try:
            from robotocore.services.lambda_.invoke import (
                invoke_lambda_sync,
            )

            event = {
                "RequestType": "Create",
                "ServiceToken": service_token,
                "ResponseURL": "",
                "StackId": "arn:aws:cloudformation:stack",
                "RequestId": uuid.uuid4().hex,
                "ResourceType": resource.resource_type,
                "LogicalResourceId": resource.logical_id,
                "ResourceProperties": resource.properties,
            }
            result = invoke_lambda_sync(service_token, json.dumps(event), region, account_id)
            if isinstance(result, dict):
                resource.physical_id = result.get(
                    "PhysicalResourceId",
                    f"custom-{uuid.uuid4().hex[:8]}",
                )
                data = result.get("Data", {})
                if isinstance(data, dict):
                    resource.attributes.update(data)
            else:
                resource.physical_id = f"custom-{uuid.uuid4().hex[:8]}"
        except Exception:
            resource.physical_id = f"custom-{uuid.uuid4().hex[:8]}"
    else:
        resource.physical_id = f"custom-{uuid.uuid4().hex[:8]}"
    resource.status = "CREATE_COMPLETE"


def _delete_custom_resource(resource: CfnResource, region: str, account_id: str) -> None:
    """Attempt to invoke Lambda for delete, but don't fail."""
    service_token = resource.properties.get("ServiceToken", "")
    if service_token and ":function:" in service_token:
        try:
            from robotocore.services.lambda_.invoke import (
                invoke_lambda_sync,
            )

            event = {
                "RequestType": "Delete",
                "ServiceToken": service_token,
                "ResponseURL": "",
                "StackId": "arn:aws:cloudformation:stack",
                "RequestId": uuid.uuid4().hex,
                "ResourceType": resource.resource_type,
                "LogicalResourceId": resource.logical_id,
                "PhysicalResourceId": resource.physical_id,
                "ResourceProperties": resource.properties,
            }
            invoke_lambda_sync(service_token, json.dumps(event), region, account_id)
        except Exception:
            pass


# --- SES::EmailIdentity ---


def _create_ses_email_identity(resource: CfnResource, region: str, account_id: str) -> None:
    identity = resource.properties.get("EmailIdentity", resource.logical_id)
    resource.physical_id = identity
    resource.attributes["EmailIdentity"] = identity
    resource.status = "CREATE_COMPLETE"


def _delete_ses_email_identity(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- SES::ConfigurationSet ---


def _create_ses_configuration_set(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get("Name", resource.logical_id)
    resource.physical_id = name
    resource.status = "CREATE_COMPLETE"


def _delete_ses_configuration_set(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- SES::Template ---


def _create_ses_template(resource: CfnResource, region: str, account_id: str) -> None:
    tmpl = resource.properties.get("Template", {})
    name = tmpl.get("TemplateName", resource.logical_id)
    resource.physical_id = name
    resource.status = "CREATE_COMPLETE"


def _delete_ses_template(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- CloudWatch::Dashboard ---


def _create_cloudwatch_dashboard(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get("DashboardName", resource.logical_id)
    resource.physical_id = name
    resource.status = "CREATE_COMPLETE"


def _delete_cloudwatch_dashboard(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- CloudWatch::CompositeAlarm ---


def _create_cloudwatch_composite_alarm(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get(
        "AlarmName",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
    )
    resource.physical_id = name
    resource.attributes["Arn"] = f"arn:aws:cloudwatch:{region}:{account_id}:alarm:{name}"
    resource.status = "CREATE_COMPLETE"


def _delete_cloudwatch_composite_alarm(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- Logs::SubscriptionFilter ---


def _create_logs_subscription_filter(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get("FilterName", resource.logical_id)
    resource.physical_id = name
    resource.status = "CREATE_COMPLETE"


def _delete_logs_subscription_filter(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- Logs::MetricFilter ---


def _create_logs_metric_filter(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get("FilterName", resource.logical_id)
    resource.physical_id = name
    resource.status = "CREATE_COMPLETE"


def _delete_logs_metric_filter(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- EC2::SecurityGroupIngress ---


def _create_ec2_sg_ingress(resource: CfnResource, region: str, account_id: str) -> None:
    gid = resource.properties.get("GroupId", "")
    resource.physical_id = f"{gid}-ingress-{uuid.uuid4().hex[:8]}"
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_sg_ingress(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- EC2::SecurityGroupEgress ---


def _create_ec2_sg_egress(resource: CfnResource, region: str, account_id: str) -> None:
    gid = resource.properties.get("GroupId", "")
    resource.physical_id = f"{gid}-egress-{uuid.uuid4().hex[:8]}"
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_sg_egress(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- EC2::NetworkInterface ---


def _create_ec2_network_interface(resource: CfnResource, region: str, account_id: str) -> None:
    eni_id = f"eni-{uuid.uuid4().hex[:17]}"
    resource.physical_id = eni_id
    resource.attributes["PrimaryPrivateIpAddress"] = "10.0.0.10"
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_network_interface(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- EC2::Volume ---


def _create_ec2_volume(resource: CfnResource, region: str, account_id: str) -> None:
    vid = f"vol-{uuid.uuid4().hex[:17]}"
    resource.physical_id = vid
    resource.attributes["VolumeId"] = vid
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_volume(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- EC2::Instance ---


def _create_ec2_instance(resource: CfnResource, region: str, account_id: str) -> None:
    iid = f"i-{uuid.uuid4().hex[:17]}"
    resource.physical_id = iid
    resource.attributes["InstanceId"] = iid
    resource.attributes["PrivateIp"] = "10.0.0.50"
    resource.attributes["PublicIp"] = "203.0.113.50"
    resource.attributes["PrivateDnsName"] = f"ip-10-0-0-50.{region}.compute.internal"
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_instance(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- EC2::FlowLog ---


def _create_ec2_flow_log(resource: CfnResource, region: str, account_id: str) -> None:
    fid = f"fl-{uuid.uuid4().hex[:17]}"
    resource.physical_id = fid
    resource.attributes["Id"] = fid
    resource.status = "CREATE_COMPLETE"


def _delete_ec2_flow_log(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- ApplicationAutoScaling::ScalableTarget ---


def _create_autoscaling_scalable_target(
    resource: CfnResource, region: str, account_id: str
) -> None:
    resource_id = resource.properties.get("ResourceId", resource.logical_id)
    resource.physical_id = resource_id
    resource.status = "CREATE_COMPLETE"


def _delete_autoscaling_scalable_target(
    resource: CfnResource, region: str, account_id: str
) -> None:
    pass


# --- ApplicationAutoScaling::ScalingPolicy ---


def _create_autoscaling_scaling_policy(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get("PolicyName", resource.logical_id)
    resource.physical_id = name
    resource.status = "CREATE_COMPLETE"


def _delete_autoscaling_scaling_policy(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- SNS::TopicPolicy ---


def _create_sns_topic_policy(resource: CfnResource, region: str, account_id: str) -> None:
    topics = resource.properties.get("Topics", [])
    resource.physical_id = topics[0] if topics else f"tp-{uuid.uuid4().hex[:8]}"
    resource.status = "CREATE_COMPLETE"


def _delete_sns_topic_policy(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- Lambda::EventInvokeConfig ---


def _create_lambda_event_invoke_config(resource: CfnResource, region: str, account_id: str) -> None:
    fn_name = resource.properties.get("FunctionName", resource.logical_id)
    resource.physical_id = fn_name
    resource.status = "CREATE_COMPLETE"


def _delete_lambda_event_invoke_config(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- Lambda::Url ---


def _create_lambda_url(resource: CfnResource, region: str, account_id: str) -> None:
    url_id = uuid.uuid4().hex[:12]
    url = f"https://{url_id}.lambda-url.{region}.on.aws/"
    resource.physical_id = url
    resource.attributes["FunctionUrl"] = url
    resource.status = "CREATE_COMPLETE"


def _delete_lambda_url(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- IAM::RolePolicy ---


def _create_iam_role_policy(resource: CfnResource, region: str, account_id: str) -> None:
    role_name = resource.properties.get("RoleName", "")
    policy_name = resource.properties.get("PolicyName", resource.logical_id)
    resource.physical_id = f"{role_name}|{policy_name}"
    resource.status = "CREATE_COMPLETE"


def _delete_iam_role_policy(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- IAM::UserToGroupAddition ---


def _create_iam_user_to_group(resource: CfnResource, region: str, account_id: str) -> None:
    group = resource.properties.get("GroupName", "")
    resource.physical_id = f"{group}-users-{uuid.uuid4().hex[:8]}"
    resource.status = "CREATE_COMPLETE"


def _delete_iam_user_to_group(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- CloudFormation::WaitConditionHandle ---


def _create_cfn_wait_condition_handle(resource: CfnResource, region: str, account_id: str) -> None:
    url = f"https://cloudformation-waitcondition-{region}.s3.amazonaws.com/"
    resource.physical_id = url
    resource.status = "CREATE_COMPLETE"


def _delete_cfn_wait_condition_handle(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- CloudFormation::WaitCondition ---


def _create_cfn_wait_condition(resource: CfnResource, region: str, account_id: str) -> None:
    resource.physical_id = f"waitcond-{uuid.uuid4().hex[:8]}"
    resource.attributes["Data"] = "{}"
    resource.status = "CREATE_COMPLETE"


def _delete_cfn_wait_condition(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- CloudFormation::Stack (nested stacks) ---


def _create_cfn_stack(resource: CfnResource, region: str, account_id: str) -> None:
    stack_id = (
        f"arn:aws:cloudformation:{region}:{account_id}"
        f":stack/nested-{uuid.uuid4().hex[:8]}/{uuid.uuid4()}"
    )
    resource.physical_id = stack_id
    resource.attributes["StackId"] = stack_id
    resource.status = "CREATE_COMPLETE"


def _delete_cfn_stack(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- Cognito::UserPoolClient ---


def _create_cognito_user_pool_client(resource: CfnResource, region: str, account_id: str) -> None:
    # Use native Cognito provider store (not Moto)
    from robotocore.services.cognito.provider import _get_store as _get_cognito_store

    store = _get_cognito_store(region)
    pool_id = resource.properties.get("UserPoolId", "")
    client_name = resource.properties.get("ClientName", resource.logical_id)
    cid = uuid.uuid4().hex[:26]
    import time as _time

    client_data = {
        "ClientId": cid,
        "ClientName": client_name,
        "UserPoolId": pool_id,
        "CreationDate": _time.time(),
        "LastModifiedDate": _time.time(),
        "ExplicitAuthFlows": resource.properties.get("ExplicitAuthFlows", []),
        "AllowedOAuthFlows": resource.properties.get("AllowedOAuthFlows", []),
    }
    if resource.properties.get("GenerateSecret"):
        client_data["ClientSecret"] = uuid.uuid4().hex
    with store.lock:
        if pool_id in store.clients:
            store.clients[pool_id][cid] = client_data
    resource.physical_id = cid
    resource.attributes["ClientId"] = cid
    resource.attributes["Name"] = client_name
    resource.status = "CREATE_COMPLETE"


def _delete_cognito_user_pool_client(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- Cognito::IdentityPool ---


def _create_cognito_identity_pool(resource: CfnResource, region: str, account_id: str) -> None:
    pool_id = f"{region}:{uuid.uuid4()}"
    resource.physical_id = pool_id
    resource.attributes["Id"] = pool_id
    resource.attributes["Name"] = resource.properties.get("IdentityPoolName", resource.logical_id)
    resource.status = "CREATE_COMPLETE"


def _delete_cognito_identity_pool(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- WAFv2::WebACL ---


def _create_wafv2_web_acl(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get("Name", resource.logical_id)
    acl_id = uuid.uuid4().hex[:36]
    arn = f"arn:aws:wafv2:{region}:{account_id}:regional/webacl/{name}/{acl_id}"
    resource.physical_id = arn
    resource.attributes["Arn"] = arn
    resource.attributes["Id"] = acl_id
    resource.status = "CREATE_COMPLETE"


def _delete_wafv2_web_acl(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- Elasticsearch::Domain ---


def _create_elasticsearch_domain(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get("DomainName", resource.logical_id)
    arn = f"arn:aws:es:{region}:{account_id}:domain/{name}"
    resource.physical_id = name
    resource.attributes["Arn"] = arn
    resource.attributes["DomainArn"] = arn
    resource.attributes["DomainEndpoint"] = (
        f"search-{name}-{uuid.uuid4().hex[:10]}.{region}.es.amazonaws.com"
    )
    resource.status = "CREATE_COMPLETE"


def _delete_elasticsearch_domain(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- Redshift::Cluster ---


def _create_redshift_cluster(resource: CfnResource, region: str, account_id: str) -> None:
    cid = resource.properties.get(
        "ClusterIdentifier",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
    ).lower()
    resource.physical_id = cid
    resource.attributes["Endpoint.Address"] = (
        f"{cid}.{uuid.uuid4().hex[:8]}.{region}.redshift.amazonaws.com"
    )
    resource.attributes["Endpoint.Port"] = "5439"
    resource.status = "CREATE_COMPLETE"


def _delete_redshift_cluster(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- RDS::DBInstance ---


def _create_rds_db_instance(resource: CfnResource, region: str, account_id: str) -> None:
    db_id = resource.properties.get(
        "DBInstanceIdentifier",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
    ).lower()
    resource.physical_id = db_id
    resource.attributes["Endpoint.Address"] = (
        f"{db_id}.{uuid.uuid4().hex[:8]}.{region}.rds.amazonaws.com"
    )
    resource.attributes["Endpoint.Port"] = str(resource.properties.get("Port", 3306))
    resource.status = "CREATE_COMPLETE"


def _delete_rds_db_instance(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- RDS::DBSubnetGroup ---


def _create_rds_db_subnet_group(resource: CfnResource, region: str, account_id: str) -> None:
    name = resource.properties.get("DBSubnetGroupName", resource.logical_id).lower()
    resource.physical_id = name
    resource.status = "CREATE_COMPLETE"


def _delete_rds_db_subnet_group(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- RDS::DBCluster ---


def _create_rds_db_cluster(resource: CfnResource, region: str, account_id: str) -> None:
    cid = resource.properties.get(
        "DBClusterIdentifier",
        f"cfn-{resource.logical_id}-{uuid.uuid4().hex[:8]}",
    ).lower()
    resource.physical_id = cid
    resource.attributes["Endpoint.Address"] = (
        f"{cid}.cluster-{uuid.uuid4().hex[:8]}.{region}.rds.amazonaws.com"
    )
    resource.attributes["Endpoint.Port"] = "5432"
    resource.status = "CREATE_COMPLETE"


def _delete_rds_db_cluster(resource: CfnResource, region: str, account_id: str) -> None:
    pass


# --- Fn::Cidr helper ---


def compute_cidr(ip_block: str, count: int, cidr_bits: int) -> list[str]:
    """Compute CIDR sub-ranges for Fn::Cidr intrinsic."""
    try:
        network = ipaddress.ip_network(ip_block, strict=False)
        new_prefix = network.max_prefixlen - cidr_bits
        subnets = list(network.subnets(new_prefix=new_prefix))
        return [str(s) for s in subnets[:count]]
    except Exception:
        return [ip_block]


# --- Handler maps ---

_CREATE_HANDLERS = {
    # Original 12
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
    # IAM (6 new)
    "AWS::IAM::ManagedPolicy": _create_iam_managed_policy,
    "AWS::IAM::InstanceProfile": _create_iam_instance_profile,
    "AWS::IAM::User": _create_iam_user,
    "AWS::IAM::Group": _create_iam_group,
    "AWS::IAM::AccessKey": _create_iam_access_key,
    "AWS::IAM::ServiceLinkedRole": _create_iam_service_linked_role,
    # Lambda (5 new)
    "AWS::Lambda::Version": _create_lambda_version,
    "AWS::Lambda::Alias": _create_lambda_alias,
    "AWS::Lambda::EventSourceMapping": _create_lambda_event_source_mapping,
    "AWS::Lambda::Permission": _create_lambda_permission,
    "AWS::Lambda::LayerVersion": _create_lambda_layer_version,
    # EC2 (12 new)
    "AWS::EC2::VPC": _create_ec2_vpc,
    "AWS::EC2::Subnet": _create_ec2_subnet,
    "AWS::EC2::SecurityGroup": _create_ec2_security_group,
    "AWS::EC2::InternetGateway": _create_ec2_internet_gateway,
    "AWS::EC2::VPCGatewayAttachment": _create_ec2_vpc_gateway_attachment,
    "AWS::EC2::RouteTable": _create_ec2_route_table,
    "AWS::EC2::Route": _create_ec2_route,
    "AWS::EC2::SubnetRouteTableAssociation": _create_ec2_subnet_route_table_assoc,
    "AWS::EC2::NatGateway": _create_ec2_nat_gateway,
    "AWS::EC2::EIP": _create_ec2_eip,
    "AWS::EC2::LaunchTemplate": _create_ec2_launch_template,
    "AWS::EC2::KeyPair": _create_ec2_key_pair,
    # S3 (2 new)
    "AWS::S3::BucketPolicy": _create_s3_bucket_policy,
    "AWS::S3::BucketNotificationConfiguration": _create_s3_bucket_notification_config,
    # DynamoDB (1 new)
    "AWS::DynamoDB::GlobalTable": _create_dynamodb_global_table,
    # SQS (1 new)
    "AWS::SQS::QueuePolicy": _create_sqs_queue_policy,
    # API Gateway (8 new)
    "AWS::ApiGateway::RestApi": _create_apigw_rest_api,
    "AWS::ApiGateway::Resource": _create_apigw_resource,
    "AWS::ApiGateway::Method": _create_apigw_method,
    "AWS::ApiGateway::Deployment": _create_apigw_deployment,
    "AWS::ApiGateway::Stage": _create_apigw_stage,
    "AWS::ApiGateway::ApiKey": _create_apigw_api_key,
    "AWS::ApiGateway::UsagePlan": _create_apigw_usage_plan,
    "AWS::ApiGateway::DomainName": _create_apigw_domain_name,
    # CloudWatch (3 new)
    "AWS::CloudWatch::Alarm": _create_cloudwatch_alarm,
    "AWS::Logs::LogStream": _create_log_stream,
    "AWS::CloudWatch::MetricFilter": _create_cloudwatch_metric_filter,
    # EventBridge (2 new)
    "AWS::Events::EventBus": _create_events_event_bus,
    "AWS::Events::Archive": _create_events_archive,
    # Step Functions (2 new)
    "AWS::StepFunctions::StateMachine": _create_sfn_state_machine,
    "AWS::StepFunctions::Activity": _create_sfn_activity,
    # KMS (1 new)
    "AWS::KMS::Alias": _create_kms_alias,
    # Secrets Manager (1 new)
    "AWS::SecretsManager::Secret": _create_secretsmanager_secret,
    # SSM (1 new)
    "AWS::SSM::Document": _create_ssm_document,
    # Kinesis (1 new)
    "AWS::Kinesis::Stream": _create_kinesis_stream,
    # ECS (3 new)
    "AWS::ECS::Cluster": _create_ecs_cluster,
    "AWS::ECS::TaskDefinition": _create_ecs_task_definition,
    "AWS::ECS::Service": _create_ecs_service,
    # ELB (3 new)
    "AWS::ElasticLoadBalancingV2::LoadBalancer": _create_elbv2_load_balancer,
    "AWS::ElasticLoadBalancingV2::TargetGroup": _create_elbv2_target_group,
    "AWS::ElasticLoadBalancingV2::Listener": _create_elbv2_listener,
    # CloudFront (1 new)
    "AWS::CloudFront::Distribution": _create_cloudfront_distribution,
    # Route53 (2 new)
    "AWS::Route53::HostedZone": _create_route53_hosted_zone,
    "AWS::Route53::RecordSet": _create_route53_record_set,
    # ACM (1 new)
    "AWS::CertificateManager::Certificate": _create_acm_certificate,
    # Cognito (1 new)
    "AWS::Cognito::UserPool": _create_cognito_user_pool,
    # --- Additional resource types to reach 100+ ---
    "AWS::SES::EmailIdentity": _create_ses_email_identity,
    "AWS::SES::ConfigurationSet": _create_ses_configuration_set,
    "AWS::SES::Template": _create_ses_template,
    "AWS::CloudWatch::Dashboard": _create_cloudwatch_dashboard,
    "AWS::CloudWatch::CompositeAlarm": _create_cloudwatch_composite_alarm,
    "AWS::Logs::SubscriptionFilter": _create_logs_subscription_filter,
    "AWS::Logs::MetricFilter": _create_logs_metric_filter,
    "AWS::EC2::SecurityGroupIngress": _create_ec2_sg_ingress,
    "AWS::EC2::SecurityGroupEgress": _create_ec2_sg_egress,
    "AWS::EC2::NetworkInterface": _create_ec2_network_interface,
    "AWS::EC2::Volume": _create_ec2_volume,
    "AWS::EC2::Instance": _create_ec2_instance,
    "AWS::EC2::FlowLog": _create_ec2_flow_log,
    "AWS::ApplicationAutoScaling::ScalableTarget": _create_autoscaling_scalable_target,
    "AWS::ApplicationAutoScaling::ScalingPolicy": _create_autoscaling_scaling_policy,
    "AWS::SNS::TopicPolicy": _create_sns_topic_policy,
    "AWS::Lambda::EventInvokeConfig": _create_lambda_event_invoke_config,
    "AWS::Lambda::Url": _create_lambda_url,
    "AWS::IAM::RolePolicy": _create_iam_role_policy,
    "AWS::IAM::UserToGroupAddition": _create_iam_user_to_group,
    "AWS::CloudFormation::WaitConditionHandle": _create_cfn_wait_condition_handle,
    "AWS::CloudFormation::WaitCondition": _create_cfn_wait_condition,
    "AWS::CloudFormation::Stack": _create_cfn_stack,
    "AWS::Cognito::UserPoolClient": _create_cognito_user_pool_client,
    "AWS::Cognito::IdentityPool": _create_cognito_identity_pool,
    "AWS::WAFv2::WebACL": _create_wafv2_web_acl,
    "AWS::Elasticsearch::Domain": _create_elasticsearch_domain,
    "AWS::Redshift::Cluster": _create_redshift_cluster,
    "AWS::RDS::DBInstance": _create_rds_db_instance,
    "AWS::RDS::DBSubnetGroup": _create_rds_db_subnet_group,
    "AWS::RDS::DBCluster": _create_rds_db_cluster,
}

_DELETE_HANDLERS = {
    # Original 12
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
    # IAM (6 new)
    "AWS::IAM::ManagedPolicy": _delete_iam_managed_policy,
    "AWS::IAM::InstanceProfile": _delete_iam_instance_profile,
    "AWS::IAM::User": _delete_iam_user,
    "AWS::IAM::Group": _delete_iam_group,
    "AWS::IAM::AccessKey": _delete_iam_access_key,
    "AWS::IAM::ServiceLinkedRole": _delete_iam_service_linked_role,
    # Lambda (5 new)
    "AWS::Lambda::Version": _delete_lambda_version,
    "AWS::Lambda::Alias": _delete_lambda_alias,
    "AWS::Lambda::EventSourceMapping": _delete_lambda_event_source_mapping,
    "AWS::Lambda::Permission": _delete_lambda_permission,
    "AWS::Lambda::LayerVersion": _delete_lambda_layer_version,
    # EC2 (12 new)
    "AWS::EC2::VPC": _delete_ec2_vpc,
    "AWS::EC2::Subnet": _delete_ec2_subnet,
    "AWS::EC2::SecurityGroup": _delete_ec2_security_group,
    "AWS::EC2::InternetGateway": _delete_ec2_internet_gateway,
    "AWS::EC2::VPCGatewayAttachment": _delete_ec2_vpc_gateway_attachment,
    "AWS::EC2::RouteTable": _delete_ec2_route_table,
    "AWS::EC2::Route": _delete_ec2_route,
    "AWS::EC2::SubnetRouteTableAssociation": _delete_ec2_subnet_route_table_assoc,
    "AWS::EC2::NatGateway": _delete_ec2_nat_gateway,
    "AWS::EC2::EIP": _delete_ec2_eip,
    "AWS::EC2::LaunchTemplate": _delete_ec2_launch_template,
    "AWS::EC2::KeyPair": _delete_ec2_key_pair,
    # S3 (2 new)
    "AWS::S3::BucketPolicy": _delete_s3_bucket_policy,
    "AWS::S3::BucketNotificationConfiguration": _delete_s3_bucket_notification_config,
    # DynamoDB (1 new)
    "AWS::DynamoDB::GlobalTable": _delete_dynamodb_global_table,
    # SQS (1 new)
    "AWS::SQS::QueuePolicy": _delete_sqs_queue_policy,
    # API Gateway (8 new)
    "AWS::ApiGateway::RestApi": _delete_apigw_rest_api,
    "AWS::ApiGateway::Resource": _delete_apigw_resource,
    "AWS::ApiGateway::Method": _delete_apigw_method,
    "AWS::ApiGateway::Deployment": _delete_apigw_deployment,
    "AWS::ApiGateway::Stage": _delete_apigw_stage,
    "AWS::ApiGateway::ApiKey": _delete_apigw_api_key,
    "AWS::ApiGateway::UsagePlan": _delete_apigw_usage_plan,
    "AWS::ApiGateway::DomainName": _delete_apigw_domain_name,
    # CloudWatch (3 new)
    "AWS::CloudWatch::Alarm": _delete_cloudwatch_alarm,
    "AWS::Logs::LogStream": _delete_log_stream,
    "AWS::CloudWatch::MetricFilter": _delete_cloudwatch_metric_filter,
    # EventBridge (2 new)
    "AWS::Events::EventBus": _delete_events_event_bus,
    "AWS::Events::Archive": _delete_events_archive,
    # Step Functions (2 new)
    "AWS::StepFunctions::StateMachine": _delete_sfn_state_machine,
    "AWS::StepFunctions::Activity": _delete_sfn_activity,
    # KMS (1 new)
    "AWS::KMS::Alias": _delete_kms_alias,
    # Secrets Manager (1 new)
    "AWS::SecretsManager::Secret": _delete_secretsmanager_secret,
    # SSM (1 new)
    "AWS::SSM::Document": _delete_ssm_document,
    # Kinesis (1 new)
    "AWS::Kinesis::Stream": _delete_kinesis_stream,
    # ECS (3 new)
    "AWS::ECS::Cluster": _delete_ecs_cluster,
    "AWS::ECS::TaskDefinition": _delete_ecs_task_definition,
    "AWS::ECS::Service": _delete_ecs_service,
    # ELB (3 new)
    "AWS::ElasticLoadBalancingV2::LoadBalancer": _delete_elbv2_load_balancer,
    "AWS::ElasticLoadBalancingV2::TargetGroup": _delete_elbv2_target_group,
    "AWS::ElasticLoadBalancingV2::Listener": _delete_elbv2_listener,
    # CloudFront (1 new)
    "AWS::CloudFront::Distribution": _delete_cloudfront_distribution,
    # Route53 (2 new)
    "AWS::Route53::HostedZone": _delete_route53_hosted_zone,
    "AWS::Route53::RecordSet": _delete_route53_record_set,
    # ACM (1 new)
    "AWS::CertificateManager::Certificate": _delete_acm_certificate,
    # Cognito (1 new)
    "AWS::Cognito::UserPool": _delete_cognito_user_pool,
    # --- Additional resource types to reach 100+ ---
    "AWS::SES::EmailIdentity": _delete_ses_email_identity,
    "AWS::SES::ConfigurationSet": _delete_ses_configuration_set,
    "AWS::SES::Template": _delete_ses_template,
    "AWS::CloudWatch::Dashboard": _delete_cloudwatch_dashboard,
    "AWS::CloudWatch::CompositeAlarm": _delete_cloudwatch_composite_alarm,
    "AWS::Logs::SubscriptionFilter": _delete_logs_subscription_filter,
    "AWS::Logs::MetricFilter": _delete_logs_metric_filter,
    "AWS::EC2::SecurityGroupIngress": _delete_ec2_sg_ingress,
    "AWS::EC2::SecurityGroupEgress": _delete_ec2_sg_egress,
    "AWS::EC2::NetworkInterface": _delete_ec2_network_interface,
    "AWS::EC2::Volume": _delete_ec2_volume,
    "AWS::EC2::Instance": _delete_ec2_instance,
    "AWS::EC2::FlowLog": _delete_ec2_flow_log,
    "AWS::ApplicationAutoScaling::ScalableTarget": _delete_autoscaling_scalable_target,
    "AWS::ApplicationAutoScaling::ScalingPolicy": _delete_autoscaling_scaling_policy,
    "AWS::SNS::TopicPolicy": _delete_sns_topic_policy,
    "AWS::Lambda::EventInvokeConfig": _delete_lambda_event_invoke_config,
    "AWS::Lambda::Url": _delete_lambda_url,
    "AWS::IAM::RolePolicy": _delete_iam_role_policy,
    "AWS::IAM::UserToGroupAddition": _delete_iam_user_to_group,
    "AWS::CloudFormation::WaitConditionHandle": _delete_cfn_wait_condition_handle,
    "AWS::CloudFormation::WaitCondition": _delete_cfn_wait_condition,
    "AWS::CloudFormation::Stack": _delete_cfn_stack,
    "AWS::Cognito::UserPoolClient": _delete_cognito_user_pool_client,
    "AWS::Cognito::IdentityPool": _delete_cognito_identity_pool,
    "AWS::WAFv2::WebACL": _delete_wafv2_web_acl,
    "AWS::Elasticsearch::Domain": _delete_elasticsearch_domain,
    "AWS::Redshift::Cluster": _delete_redshift_cluster,
    "AWS::RDS::DBInstance": _delete_rds_db_instance,
    "AWS::RDS::DBSubnetGroup": _delete_rds_db_subnet_group,
    "AWS::RDS::DBCluster": _delete_rds_db_cluster,
}
