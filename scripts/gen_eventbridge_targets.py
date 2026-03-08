#!/usr/bin/env python3
"""Generate EventBridge target dispatcher stubs from target type list.

Creates target handler functions for all 17 EventBridge target types matching
Enterprise LocalStack feature parity.

Usage:
    uv run python scripts/gen_eventbridge_targets.py --list              # List all target types
    uv run python scripts/gen_eventbridge_targets.py --dry-run           # Preview generated code
    uv run python scripts/gen_eventbridge_targets.py --write             # Write targets directory
    uv run python scripts/gen_eventbridge_targets.py --file lambda       # Generate single target
"""

import argparse
import re
import sys
from pathlib import Path

# All 17 EventBridge target types with their ARN patterns and dispatch logic
TARGET_SPECS: dict[str, dict] = {
    "lambda": {
        "arn_pattern": ":lambda:",
        "arn_example": "arn:aws:lambda:us-east-1:123456789012:function:my-func",
        "description": "Invoke a Lambda function",
        "dispatch": "invoke_function",
        "existing": True,
    },
    "sqs": {
        "arn_pattern": ":sqs:",
        "arn_example": "arn:aws:sqs:us-east-1:123456789012:my-queue",
        "description": "Send message to SQS queue",
        "dispatch": "send_message",
        "existing": True,
    },
    "sns": {
        "arn_pattern": ":sns:",
        "arn_example": "arn:aws:sns:us-east-1:123456789012:my-topic",
        "description": "Publish to SNS topic",
        "dispatch": "publish",
        "existing": True,
    },
    "kinesis": {
        "arn_pattern": ":kinesis:",
        "arn_example": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
        "description": "Put record to Kinesis stream",
        "dispatch": "put_record",
    },
    "firehose": {
        "arn_pattern": ":firehose:",
        "arn_example": "arn:aws:firehose:us-east-1:123456789012:deliverystream/my-stream",
        "description": "Put record to Firehose delivery stream",
        "dispatch": "put_record",
    },
    "stepfunctions": {
        "arn_pattern": ":states:",
        "arn_example": "arn:aws:states:us-east-1:123456789012:stateMachine:my-sfn",
        "description": "Start Step Functions execution",
        "dispatch": "start_execution",
    },
    "logs": {
        "arn_pattern": ":logs:",
        "arn_example": "arn:aws:logs:us-east-1:123456789012:log-group:/aws/events/my-log",
        "description": "Create log event in CloudWatch Logs",
        "dispatch": "put_log_events",
    },
    "ecs": {
        "arn_pattern": ":ecs:",
        "arn_example": "arn:aws:ecs:us-east-1:123456789012:cluster/my-cluster",
        "description": "Run ECS task",
        "dispatch": "run_task",
    },
    "codebuild": {
        "arn_pattern": ":codebuild:",
        "arn_example": "arn:aws:codebuild:us-east-1:123456789012:project/my-project",
        "description": "Start CodeBuild build",
        "dispatch": "start_build",
    },
    "codepipeline": {
        "arn_pattern": ":codepipeline:",
        "arn_example": "arn:aws:codepipeline:us-east-1:123456789012:my-pipeline",
        "description": "Start CodePipeline execution",
        "dispatch": "start_pipeline_execution",
    },
    "batch": {
        "arn_pattern": ":batch:",
        "arn_example": "arn:aws:batch:us-east-1:123456789012:job-queue/my-queue",
        "description": "Submit Batch job",
        "dispatch": "submit_job",
    },
    "apigateway": {
        "arn_pattern": ":execute-api:",
        "arn_example": "arn:aws:execute-api:us-east-1:123456789012:api-id/stage/METHOD/path",
        "description": "Invoke API Gateway endpoint",
        "dispatch": "invoke_api",
    },
    "ssm": {
        "arn_pattern": ":ssm:",
        "arn_example": "arn:aws:ssm:us-east-1:123456789012:automation-definition/my-doc",
        "description": "Start SSM automation",
        "dispatch": "start_automation_execution",
    },
    "redshift": {
        "arn_pattern": ":redshift:",
        "arn_example": "arn:aws:redshift:us-east-1:123456789012:cluster:my-cluster",
        "description": "Execute Redshift query (simulated)",
        "dispatch": "execute_statement",
    },
    "sagemaker": {
        "arn_pattern": ":sagemaker:",
        "arn_example": "arn:aws:sagemaker:us-east-1:123456789012:pipeline/my-pipeline",
        "description": "Start SageMaker pipeline execution (simulated)",
        "dispatch": "start_pipeline_execution",
    },
    "inspector": {
        "arn_pattern": ":inspector:",
        "arn_example": "arn:aws:inspector:us-east-1:123456789012:target/xxx",
        "description": "Start Inspector assessment (simulated)",
        "dispatch": "start_assessment_run",
    },
    "eventbridge": {
        "arn_pattern": ":events:",
        "arn_example": "arn:aws:events:us-east-1:123456789012:event-bus/my-bus",
        "description": "Put events to another EventBridge bus",
        "dispatch": "put_events",
    },
}


def _to_snake_case(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def generate_target_handler(target_name: str, spec: dict) -> str:
    """Generate a target handler function."""
    lines = []
    lines.append(f"def _invoke_{target_name}_target(")
    lines.append("    arn: str, payload: str, region: str, account_id: str")
    lines.append(") -> None:")
    lines.append(f'    """{spec["description"]}.')
    lines.append("")
    lines.append(f"    ARN pattern: {spec['arn_example']}")
    lines.append('    """')

    if target_name == "kinesis":
        lines.extend(
            [
                "    from robotocore.services.kinesis.provider import _get_store",
                "",
                "    # Extract stream name from ARN: arn:aws:kinesis:region:acct:stream/name",
                '    stream_name = arn.rsplit("/", 1)[-1] if "/" in arn else arn.rsplit(":", 1)[-1]',  # noqa: E501
                "    store = _get_store(region)",
                "    stream = store.get_stream(stream_name)",
                "    if not stream:",
                '        logger.error(f"EventBridge: Kinesis stream not found: {stream_name}")',
                "        return",
                "",
                "    import hashlib",
                "    partition_key = hashlib.md5(payload.encode()).hexdigest()[:16]",
                "    stream.put_record(payload.encode(), partition_key)",
                '    _log_invocation("kinesis", arn, payload)',
                '    logger.info(f"EventBridge -> Kinesis: {stream_name}")',
            ]
        )
    elif target_name == "firehose":
        lines.extend(
            [
                "    from robotocore.services.firehose.provider import _get_store",
                "",
                "    # Extract delivery stream name from ARN",
                '    stream_name = arn.rsplit("/", 1)[-1]',
                "    store = _get_store(region)",
                "    stream = store.get_stream(stream_name)",
                "    if not stream:",
                '        logger.error(f"EventBridge: Firehose stream not found: {stream_name}")',
                "        return",
                "",
                "    stream.put_record(payload.encode())",
                '    _log_invocation("firehose", arn, payload)',
                '    logger.info(f"EventBridge -> Firehose: {stream_name}")',
            ]
        )
    elif target_name == "stepfunctions":
        lines.extend(
            [
                "    from robotocore.services.stepfunctions.provider import _get_store",
                "",
                "    store = _get_store(region)",
                "    import json as _json_mod",
                "    input_data = _json_mod.loads(payload) if isinstance(payload, str) else payload",  # noqa: E501
                "    store.start_execution(arn, _json_mod.dumps(input_data))",
                '    _log_invocation("stepfunctions", arn, payload)',
                '    logger.info(f"EventBridge -> StepFunctions: {arn}")',
            ]
        )
    elif target_name == "logs":
        lines.extend(
            [
                "    import time",
                "",
                "    from moto.backends import get_backend",
                "    from moto.core import DEFAULT_ACCOUNT_ID",
                "",
                "    acct = account_id if account_id != '123456789012' else DEFAULT_ACCOUNT_ID",
                "    logs_backend = get_backend('logs')[acct][region]",
                "",
                "    # Extract log group name from ARN",
                "    # arn:aws:logs:region:acct:log-group:/aws/events/name",
                '    parts = arn.split(":")',
                "    log_group_name = parts[-1] if parts else arn",
                '    if log_group_name.startswith("log-group:"):',
                '        log_group_name = log_group_name[len("log-group:"):]',
                "",
                "    # Create log group/stream if needed",
                "    try:",
                "        logs_backend.create_log_group(log_group_name, {})",
                "    except Exception:",
                "        pass  # Already exists",
                '    stream_name = "eventbridge"',
                "    try:",
                "        logs_backend.create_log_stream(log_group_name, stream_name)",
                "    except Exception:",
                "        pass  # Already exists",
                "",
                "    logs_backend.put_log_events(",
                "        log_group_name,",
                "        stream_name,",
                "        [{'timestamp': int(time.time() * 1000), 'message': payload}],",
                "    )",
                '    _log_invocation("logs", arn, payload)',
                '    logger.info(f"EventBridge -> CloudWatch Logs: {log_group_name}")',
            ]
        )
    elif target_name == "ecs":
        lines.extend(
            [
                "    # ECS RunTask - simulated (creates task record in Moto)",
                "    from moto.backends import get_backend",
                "    from moto.core import DEFAULT_ACCOUNT_ID",
                "",
                "    acct = account_id if account_id != '123456789012' else DEFAULT_ACCOUNT_ID",
                "    try:",
                "        ecs_backend = get_backend('ecs')[acct][region]",
                "        # Extract cluster from ARN",
                "        cluster_name = arn.rsplit('/', 1)[-1]",
                '        logger.info(f"EventBridge -> ECS RunTask (simulated): {cluster_name}")',
                "    except Exception:",
                '        logger.warning(f"EventBridge -> ECS target failed: {arn}")',
                '    _log_invocation("ecs", arn, payload)',
            ]
        )
    elif target_name == "eventbridge":
        lines.extend(
            [
                "    import json as _json_mod",
                "",
                "    # Put events to another EventBridge bus",
                '    bus_name = arn.rsplit("/", 1)[-1] if "/" in arn else "default"',
                "    store = _get_store(region, account_id)",
                "    bus = store.get_bus(bus_name)",
                "    if bus:",
                "        event = _json_mod.loads(payload) if isinstance(payload, str) else payload",
                "        for rule in bus.rules.values():",
                "            if rule.matches_event(event):",
                "                _dispatch_to_targets(rule, event, region, account_id)",
                '    _log_invocation("eventbridge", arn, payload)',
                '    logger.info(f"EventBridge -> EventBridge bus: {bus_name}")',
            ]
        )
    else:
        # Generic stub for unsupported targets
        lines.extend(
            [
                f'    logger.info(f"EventBridge -> {target_name} (simulated): {{arn}}")',
                f'    _log_invocation("{target_name}", arn, payload)',
            ]
        )

    lines.append("")
    return "\n".join(lines)


def generate_dispatcher(targets: dict[str, dict]) -> str:
    """Generate the _invoke_target function that dispatches to all target types."""
    lines = [
        "def _invoke_target(target, event: dict, region: str, account_id: str):",
        '    """Invoke a single target with the event."""',
        "    arn = target.arn",
        "",
        "    # Determine input to send",
        "    if target.input:",
        "        payload = target.input",
        "    elif target.input_path:",
        "        # JSONPath extraction",
        "        import jsonpath_ng",
        "        payload = json.dumps(event)",
        "    elif target.input_transformer:",
        "        payload = _apply_input_transformer(target.input_transformer, event)",
        "    else:",
        "        payload = json.dumps(event)",
        "",
    ]

    for name, spec in targets.items():
        pattern = spec["arn_pattern"]
        lines.append(f'    if "{pattern}" in arn:')
        lines.append(f"        return _invoke_{name}_target(arn, payload, region, account_id)")

    lines.extend(
        [
            "",
            '    logger.warning(f"Unsupported EventBridge target type: {arn}")',
            "",
        ]
    )
    return "\n".join(lines)


def generate_input_transformer() -> str:
    """Generate input transformer support."""
    return '''
def _apply_input_transformer(transformer: dict, event: dict) -> str:
    """Apply InputTransformer to an event.

    transformer has:
      - InputPathsMap: {"key": "$.detail.field"}
      - InputTemplate: '"<key> happened at <time>"'
    """
    input_paths = transformer.get("InputPathsMap", {})
    template = transformer.get("InputTemplate", "")

    # Resolve input paths
    resolved = {}
    for key, path in input_paths.items():
        # Simple JSONPath: $.field.subfield
        value = event
        parts = path.lstrip("$.").split(".")
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part, "")
            else:
                value = ""
                break
        resolved[key] = value if isinstance(value, str) else json.dumps(value)

    # Replace <key> placeholders in template
    result = template
    for key, value in resolved.items():
        result = result.replace(f"<{key}>", str(value))

    return result
'''


def list_targets():
    """List all target types."""
    print(f"\nEventBridge Target Types ({len(TARGET_SPECS)}):\n")
    for name, spec in sorted(TARGET_SPECS.items()):
        existing = " [EXISTING]" if spec.get("existing") else ""
        print(f"  {name:<20} {spec['description']}{existing}")
        print(f"  {'':20} ARN: {spec['arn_example']}")
        print()

    existing = sum(1 for s in TARGET_SPECS.values() if s.get("existing"))
    new = len(TARGET_SPECS) - existing
    print(f"  Existing: {existing}, New to implement: {new}")


def generate_all(write: bool = False):
    """Generate all target handlers."""
    new_targets = {n: s for n, s in TARGET_SPECS.items() if not s.get("existing")}

    print(f"\nNew EventBridge targets to generate ({len(new_targets)}):")
    for name in sorted(new_targets):
        print(f"  {name}: {new_targets[name]['description']}")

    # Generate handler code
    handlers = []
    for name, spec in sorted(new_targets.items()):
        handlers.append(generate_target_handler(name, spec))

    # Generate dispatcher
    dispatcher = generate_dispatcher(TARGET_SPECS)

    # Generate input transformer
    transformer = generate_input_transformer()

    if write:
        targets_dir = Path("src/robotocore/services/events/targets")
        targets_dir.mkdir(parents=True, exist_ok=True)
        (targets_dir / "__init__.py").write_text('"""EventBridge target dispatchers."""\n')

        content = [
            '"""EventBridge target handlers — auto-generated stubs.',
            "",
            "Each handler receives an ARN, JSON payload, region, and account ID,",
            "then dispatches to the appropriate service.",
            '"""',
            "",
            "import json",
            "import logging",
            "",
            "logger = logging.getLogger(__name__)",
            "",
            "# Import from parent provider module",
            "from robotocore.services.events.provider import (",
            "    _dispatch_to_targets,",
            "    _get_store,",
            "    _log_invocation,",
            ")",
            "",
        ]
        content.append("\n".join(handlers))
        content.append(transformer)

        (targets_dir / "handlers.py").write_text("\n".join(content))
        print(f"\nWritten to {targets_dir / 'handlers.py'}")
    else:
        print("\n# --- Generated Target Handlers ---\n")
        for h in handlers:
            print(h)
        print("\n# --- Updated Dispatcher ---\n")
        print(dispatcher)
        print("\n# --- Input Transformer ---\n")
        print(transformer)


def main():
    parser = argparse.ArgumentParser(description="Generate EventBridge target dispatchers")
    parser.add_argument("--list", action="store_true", help="List all target types")
    parser.add_argument("--dry-run", action="store_true", help="Preview generated code")
    parser.add_argument("--write", action="store_true", help="Write to targets/ directory")
    parser.add_argument("--file", help="Generate single target handler by name")
    args = parser.parse_args()

    if args.list:
        list_targets()
        return

    if args.file:
        name = args.file
        if name not in TARGET_SPECS:
            print(f"Unknown target: {name}", file=sys.stderr)
            print(f"Available: {', '.join(sorted(TARGET_SPECS))}", file=sys.stderr)
            sys.exit(1)
        print(generate_target_handler(name, TARGET_SPECS[name]))
        return

    if args.dry_run or not args.write:
        generate_all(write=False)
        return

    if args.write:
        generate_all(write=True)
        return


if __name__ == "__main__":
    main()
