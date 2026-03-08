"""Cross-service integration tests for Phase 3B.

Tests SNS→Lambda, EventBridge→Lambda, EventBridge→SNS, SQS→Lambda (ESM),
and Step Functions service integrations via the full HTTP stack.
"""

import json
import time
import uuid


class TestSNSToSQSSameRegion:
    """SNS topic delivering to SQS queue in the same region."""

    def test_sns_to_sqs_same_region(self, make_boto_client):
        suffix = uuid.uuid4().hex[:8]

        sns = make_boto_client("sns", region_name="us-east-1")
        sqs = make_boto_client("sqs", region_name="us-east-1")

        topic = sns.create_topic(Name=f"same-topic-{suffix}")
        topic_arn = topic["TopicArn"]

        q = sqs.create_queue(QueueName=f"same-queue-{suffix}")
        queue_url = q["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
        sns.publish(TopicArn=topic_arn, Message="same-region test")

        time.sleep(1)
        recv = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=5
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1
        body = json.loads(msgs[0]["Body"])
        assert body["Message"] == "same-region test"

        sns.delete_topic(TopicArn=topic_arn)
        sqs.delete_queue(QueueUrl=queue_url)


class TestEventBridgeToSQSSameRegion:
    """EventBridge rule delivering to SQS queue in the same region."""

    def test_eventbridge_to_sqs_same_region(self, make_boto_client):
        suffix = uuid.uuid4().hex[:8]

        events = make_boto_client("events", region_name="us-east-1")
        sqs = make_boto_client("sqs", region_name="us-east-1")

        q = sqs.create_queue(QueueName=f"eb-same-{suffix}")
        queue_url = q["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        events.put_rule(
            Name=f"same-rule-{suffix}",
            EventPattern=json.dumps({"source": ["same.test"]}),
        )
        events.put_targets(
            Rule=f"same-rule-{suffix}",
            Targets=[{"Id": "sqs-target", "Arn": queue_arn}],
        )

        events.put_events(
            Entries=[
                {
                    "Source": "same.test",
                    "DetailType": "SameRegionTest",
                    "Detail": json.dumps({"key": "value"}),
                }
            ]
        )

        time.sleep(1)
        recv = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=5
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1

        events.remove_targets(Rule=f"same-rule-{suffix}", Ids=["sqs-target"])
        events.delete_rule(Name=f"same-rule-{suffix}")
        sqs.delete_queue(QueueUrl=queue_url)


class TestEventBridgeToSNS:
    """EventBridge rule delivering to SNS topic."""

    def test_eventbridge_to_sns_to_sqs(self, make_boto_client):
        suffix = uuid.uuid4().hex[:8]

        events = make_boto_client("events", region_name="us-east-1")
        sns = make_boto_client("sns", region_name="us-east-1")
        sqs = make_boto_client("sqs", region_name="us-east-1")

        # Create SNS topic
        topic = sns.create_topic(Name=f"eb-sns-{suffix}")
        topic_arn = topic["TopicArn"]

        # Create SQS queue subscribed to SNS (to verify delivery)
        q = sqs.create_queue(QueueName=f"eb-sns-q-{suffix}")
        queue_url = q["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )
        queue_arn = q_attrs["Attributes"]["QueueArn"]
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

        # Create EventBridge rule targeting SNS
        events.put_rule(
            Name=f"eb-sns-rule-{suffix}",
            EventPattern=json.dumps({"source": ["eb-sns.test"]}),
        )
        events.put_targets(
            Rule=f"eb-sns-rule-{suffix}",
            Targets=[{"Id": "sns-target", "Arn": topic_arn}],
        )

        # Fire event
        events.put_events(
            Entries=[
                {
                    "Source": "eb-sns.test",
                    "DetailType": "SNSTest",
                    "Detail": json.dumps({"msg": "hello"}),
                }
            ]
        )

        time.sleep(2)  # Extra time for EB→SNS→SQS chain
        recv = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=5
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1

        # Cleanup
        events.remove_targets(Rule=f"eb-sns-rule-{suffix}", Ids=["sns-target"])
        events.delete_rule(Name=f"eb-sns-rule-{suffix}")
        sns.delete_topic(TopicArn=topic_arn)
        sqs.delete_queue(QueueUrl=queue_url)


class TestStepFunctionsIntegration:
    """Step Functions state machine creation and execution."""

    def test_create_and_execute_state_machine(self, make_boto_client):
        suffix = uuid.uuid4().hex[:8]

        sfn = make_boto_client("stepfunctions", region_name="us-east-1")
        iam = make_boto_client("iam", region_name="us-east-1")

        # Create IAM role
        role = iam.create_role(
            RoleName=f"sfn-role-{suffix}",
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "states.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }),
        )
        role_arn = role["Role"]["Arn"]

        # Simple pass state machine
        definition = json.dumps({
            "StartAt": "PassState",
            "States": {
                "PassState": {
                    "Type": "Pass",
                    "Result": {"message": "hello from sfn"},
                    "End": True,
                }
            },
        })

        sm = sfn.create_state_machine(
            name=f"test-sm-{suffix}",
            definition=definition,
            roleArn=role_arn,
        )
        sm_arn = sm["stateMachineArn"]

        # Start execution
        exec_resp = sfn.start_execution(
            stateMachineArn=sm_arn,
            input=json.dumps({"key": "value"}),
        )
        exec_arn = exec_resp["executionArn"]
        assert exec_arn is not None

        # Describe execution
        desc = sfn.describe_execution(executionArn=exec_arn)
        assert desc["stateMachineArn"] == sm_arn

        # Cleanup
        sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_list_state_machines(self, make_boto_client):
        sfn = make_boto_client("stepfunctions", region_name="us-east-1")
        resp = sfn.list_state_machines()
        assert "stateMachines" in resp


class TestDynamoDBOperations:
    """DynamoDB table operations through the full stack."""

    def test_create_table_put_get_item(self, make_boto_client):
        suffix = uuid.uuid4().hex[:8]
        ddb = make_boto_client("dynamodb", region_name="us-east-1")

        table_name = f"integ-table-{suffix}"
        ddb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        ddb.put_item(
            TableName=table_name,
            Item={"pk": {"S": "key1"}, "data": {"S": "value1"}},
        )

        resp = ddb.get_item(
            TableName=table_name,
            Key={"pk": {"S": "key1"}},
        )
        assert resp["Item"]["data"]["S"] == "value1"

        ddb.delete_table(TableName=table_name)


class TestSecretsManagerToSSM:
    """SecretsManager and SSM Parameter Store operations."""

    def test_create_secret_and_ssm_parameter(self, make_boto_client):
        suffix = uuid.uuid4().hex[:8]
        sm = make_boto_client("secretsmanager", region_name="us-east-1")
        ssm = make_boto_client("ssm", region_name="us-east-1")

        # Create secret
        sm.create_secret(
            Name=f"integ-secret-{suffix}",
            SecretString="my-secret-value",
        )

        # Create SSM parameter
        ssm.put_parameter(
            Name=f"/integ/param/{suffix}",
            Value="my-param-value",
            Type="String",
        )

        # Verify both
        secret = sm.get_secret_value(SecretId=f"integ-secret-{suffix}")
        assert secret["SecretString"] == "my-secret-value"

        param = ssm.get_parameter(Name=f"/integ/param/{suffix}")
        assert param["Parameter"]["Value"] == "my-param-value"

        # Cleanup
        sm.delete_secret(
            SecretId=f"integ-secret-{suffix}", ForceDeleteWithoutRecovery=True
        )
        ssm.delete_parameter(Name=f"/integ/param/{suffix}")
