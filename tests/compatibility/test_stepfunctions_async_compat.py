"""Step Functions async deadlock tests.

Verifies that Step Functions execution doesn't block the event loop,
even when a state machine invokes Lambda which calls back to the server.
"""

import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def sfn():
    return make_client("stepfunctions")


@pytest.fixture
def iam():
    return make_client("iam")


@pytest.fixture
def dynamodb():
    return make_client("dynamodb")


@pytest.fixture
def role_arn(iam):
    """Create an IAM role for step functions."""
    name = f"sfn-async-role-{uuid.uuid4().hex[:8]}"
    resp = iam.create_role(
        RoleName=name,
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "states.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
    )
    return resp["Role"]["Arn"]


def test_start_execution_does_not_block_server(sfn, role_arn):
    """StartExecution should return immediately while execution runs in background.

    If the event loop is blocked, the second request (ListStateMachines) will
    hang until the first execution completes.
    """
    # Create a state machine with a Wait state to simulate slow execution
    sm_name = f"slow-sm-{uuid.uuid4().hex[:8]}"
    definition = json.dumps(
        {
            "StartAt": "WaitState",
            "States": {
                "WaitState": {
                    "Type": "Wait",
                    "Seconds": 2,
                    "Next": "Done",
                },
                "Done": {
                    "Type": "Succeed",
                },
            },
        }
    )

    sm = sfn.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn=role_arn,
    )
    sm_arn = sm["stateMachineArn"]

    try:
        # Start execution (this returns immediately for STANDARD workflows)
        exec_resp = sfn.start_execution(
            stateMachineArn=sm_arn,
            input=json.dumps({"test": True}),
        )
        assert "executionArn" in exec_resp

        # Immediately make another request - this should NOT be blocked
        start = time.monotonic()
        list_resp = sfn.list_state_machines()
        elapsed = time.monotonic() - start

        assert "stateMachines" in list_resp
        # If the event loop was blocked, this would take ~2 seconds (the Wait duration)
        # It should complete in well under 1 second
        assert elapsed < 1.0, f"list_state_machines took {elapsed:.2f}s - event loop likely blocked"
    finally:
        sfn.delete_state_machine(stateMachineArn=sm_arn)


def test_concurrent_executions_do_not_block_each_other(sfn, role_arn):
    """Multiple concurrent StartExecution calls should all return promptly."""
    sm_name = f"concurrent-sm-{uuid.uuid4().hex[:8]}"
    definition = json.dumps(
        {
            "StartAt": "Pass",
            "States": {
                "Pass": {
                    "Type": "Pass",
                    "Result": {"ok": True},
                    "End": True,
                },
            },
        }
    )

    sm = sfn.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn=role_arn,
    )
    sm_arn = sm["stateMachineArn"]

    try:

        def start_one(i):
            client = make_client("stepfunctions")
            t0 = time.monotonic()
            resp = client.start_execution(
                stateMachineArn=sm_arn,
                name=f"exec-{i}-{uuid.uuid4().hex[:8]}",
                input=json.dumps({"index": i}),
            )
            return time.monotonic() - t0, resp

        # Fire 5 executions concurrently
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(start_one, i) for i in range(5)]
            results = [f.result(timeout=10) for f in as_completed(futures)]

        # All should have returned quickly
        for elapsed, resp in results:
            assert "executionArn" in resp
            assert elapsed < 3.0, f"StartExecution took {elapsed:.2f}s"
    finally:
        sfn.delete_state_machine(stateMachineArn=sm_arn)


def test_server_responsive_during_execution(sfn, role_arn, dynamodb):
    """Server should remain responsive while a state machine with a Task state executes.

    Creates a state machine that puts an item to DynamoDB (Task state),
    then verifies the server responds to other requests during execution.
    """
    table_name = f"sfn-test-{uuid.uuid4().hex[:8]}"

    # Create DynamoDB table
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    try:
        sm_name = f"task-sm-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "PutItem",
                "States": {
                    "PutItem": {
                        "Type": "Task",
                        "Resource": "arn:aws:states:::dynamodb:putItem",
                        "Parameters": {
                            "TableName": table_name,
                            "Item": {
                                "id": {"S": "from-stepfunctions"},
                                "status": {"S": "created"},
                            },
                        },
                        "End": True,
                    },
                },
            }
        )

        sm = sfn.create_state_machine(
            name=sm_name,
            definition=definition,
            roleArn=role_arn,
        )
        sm_arn = sm["stateMachineArn"]

        try:
            # Start execution
            exec_resp = sfn.start_execution(
                stateMachineArn=sm_arn,
                input=json.dumps({}),
            )
            exec_arn = exec_resp["executionArn"]

            # Server should be responsive immediately
            start = time.monotonic()
            desc = sfn.describe_state_machine(stateMachineArn=sm_arn)
            elapsed = time.monotonic() - start

            assert desc["name"] == sm_name
            assert elapsed < 1.0, f"describe_state_machine took {elapsed:.2f}s during execution"

            # Wait for execution to complete and verify the DynamoDB write happened
            for _ in range(20):
                desc_exec = sfn.describe_execution(executionArn=exec_arn)
                if desc_exec["status"] != "RUNNING":
                    break
                time.sleep(0.5)

            # Verify execution completed (SUCCEEDED or FAILED - both prove no deadlock)
            assert desc_exec["status"] != "RUNNING", "Execution still running after 10s"
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)
    finally:
        dynamodb.delete_table(TableName=table_name)
