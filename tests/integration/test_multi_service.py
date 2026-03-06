"""Cross-service integration tests exercising the ASGI app directly.

These tests verify that multiple AWS services work together when accessed
through the full HTTP stack (gateway -> router -> providers).
"""

import json
import time
import uuid


class TestS3Operations:
    """S3 bucket and object operations through the full stack."""

    def test_create_bucket_put_get_object(self, make_boto_client):
        s3 = make_boto_client("s3")
        bucket = f"integ-test-{uuid.uuid4().hex[:8]}"

        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="hello.txt", Body=b"world")

        resp = s3.get_object(Bucket=bucket, Key="hello.txt")
        body = resp["Body"].read()
        assert body == b"world"

        # Cleanup
        s3.delete_object(Bucket=bucket, Key="hello.txt")
        s3.delete_bucket(Bucket=bucket)

    def test_list_objects(self, make_boto_client):
        s3 = make_boto_client("s3")
        bucket = f"integ-list-{uuid.uuid4().hex[:8]}"

        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="a.txt", Body=b"a")
        s3.put_object(Bucket=bucket, Key="b.txt", Body=b"b")

        resp = s3.list_objects_v2(Bucket=bucket)
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        assert "a.txt" in keys
        assert "b.txt" in keys

        # Cleanup
        for key in keys:
            s3.delete_object(Bucket=bucket, Key=key)
        s3.delete_bucket(Bucket=bucket)

    def test_head_object(self, make_boto_client):
        s3 = make_boto_client("s3")
        bucket = f"integ-head-{uuid.uuid4().hex[:8]}"

        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="meta.txt", Body=b"content")

        resp = s3.head_object(Bucket=bucket, Key="meta.txt")
        assert resp["ContentLength"] == 7

        s3.delete_object(Bucket=bucket, Key="meta.txt")
        s3.delete_bucket(Bucket=bucket)


class TestDynamoDBOperations:
    """DynamoDB table and item operations through the full stack."""

    def test_create_table_put_get_item(self, make_boto_client):
        ddb = make_boto_client("dynamodb")
        table_name = f"integ-test-{uuid.uuid4().hex[:8]}"

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

    def test_query_items(self, make_boto_client):
        ddb = make_boto_client("dynamodb")
        table_name = f"integ-query-{uuid.uuid4().hex[:8]}"

        ddb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        for i in range(3):
            ddb.put_item(
                TableName=table_name,
                Item={
                    "pk": {"S": "user1"},
                    "sk": {"S": f"item-{i}"},
                    "val": {"N": str(i)},
                },
            )

        resp = ddb.query(
            TableName=table_name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "user1"}},
        )
        assert resp["Count"] == 3

        ddb.delete_table(TableName=table_name)

    def test_batch_write_and_scan(self, make_boto_client):
        ddb = make_boto_client("dynamodb")
        table_name = f"integ-batch-{uuid.uuid4().hex[:8]}"

        ddb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        items = [{"PutRequest": {"Item": {"pk": {"S": f"k{i}"}}}} for i in range(5)]
        ddb.batch_write_item(RequestItems={table_name: items})

        resp = ddb.scan(TableName=table_name)
        assert resp["Count"] == 5

        ddb.delete_table(TableName=table_name)


class TestSQSSNSIntegration:
    """SQS and SNS working together through the full stack."""

    def test_sqs_send_receive_delete(self, make_boto_client):
        sqs = make_boto_client("sqs")
        q_name = f"integ-sqs-{uuid.uuid4().hex[:8]}"

        resp = sqs.create_queue(QueueName=q_name)
        queue_url = resp["QueueUrl"]

        sqs.send_message(QueueUrl=queue_url, MessageBody="hello")
        recv = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
        msgs = recv.get("Messages", [])
        assert len(msgs) == 1
        assert msgs[0]["Body"] == "hello"

        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=msgs[0]["ReceiptHandle"],
        )
        sqs.delete_queue(QueueUrl=queue_url)

    def test_sns_publish_to_sqs_subscription(self, make_boto_client):
        sns = make_boto_client("sns")
        sqs = make_boto_client("sqs")
        suffix = uuid.uuid4().hex[:8]

        # Create topic and queue
        topic_resp = sns.create_topic(Name=f"integ-topic-{suffix}")
        topic_arn = topic_resp["TopicArn"]

        q_resp = sqs.create_queue(QueueName=f"integ-queue-{suffix}")
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        # Subscribe SQS to SNS
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

        # Publish to SNS
        sns.publish(
            TopicArn=topic_arn,
            Message="cross-service test",
        )

        # Give a moment for delivery
        time.sleep(1)

        # Receive from SQS
        recv = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1
        body = json.loads(msgs[0]["Body"])
        assert body["Message"] == "cross-service test"

        # Cleanup
        sns.delete_topic(TopicArn=topic_arn)
        sqs.delete_queue(QueueUrl=queue_url)


class TestSQSFifoQueue:
    """FIFO queue operations."""

    def test_fifo_ordering(self, make_boto_client):
        sqs = make_boto_client("sqs")
        q_name = f"integ-fifo-{uuid.uuid4().hex[:8]}.fifo"

        resp = sqs.create_queue(
            QueueName=q_name,
            Attributes={
                "FifoQueue": "true",
                "ContentBasedDeduplication": "true",
            },
        )
        queue_url = resp["QueueUrl"]

        for i in range(3):
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=f"msg-{i}",
                MessageGroupId="group1",
            )

        recv = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        msgs = recv.get("Messages", [])
        assert len(msgs) == 3
        assert [m["Body"] for m in msgs] == [
            "msg-0",
            "msg-1",
            "msg-2",
        ]

        sqs.delete_queue(QueueUrl=queue_url)


class TestIAMAndSTS:
    """IAM and STS operations through the full stack."""

    def test_sts_get_caller_identity(self, make_boto_client):
        sts = make_boto_client("sts")
        resp = sts.get_caller_identity()
        assert "Account" in resp
        assert "Arn" in resp

    def test_iam_create_role(self, make_boto_client):
        iam = make_boto_client("iam")
        role_name = f"integ-role-{uuid.uuid4().hex[:8]}"
        trust = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "lambda.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        )

        resp = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=trust,
        )
        assert resp["Role"]["RoleName"] == role_name

        iam.delete_role(RoleName=role_name)


class TestKMSAndSecretsManager:
    """KMS and Secrets Manager operations."""

    def test_kms_create_key_encrypt_decrypt(self, make_boto_client):
        kms = make_boto_client("kms")

        key_resp = kms.create_key(Description="integ-test-key")
        key_id = key_resp["KeyMetadata"]["KeyId"]

        enc_resp = kms.encrypt(KeyId=key_id, Plaintext=b"secret data")
        assert "CiphertextBlob" in enc_resp

        dec_resp = kms.decrypt(CiphertextBlob=enc_resp["CiphertextBlob"])
        assert dec_resp["Plaintext"] == b"secret data"

    def test_secretsmanager_create_get_secret(self, make_boto_client):
        sm = make_boto_client("secretsmanager")
        name = f"integ-secret-{uuid.uuid4().hex[:8]}"

        sm.create_secret(Name=name, SecretString="my-secret-value")

        resp = sm.get_secret_value(SecretId=name)
        assert resp["SecretString"] == "my-secret-value"

        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)


class TestSSMParameterStore:
    """SSM Parameter Store operations."""

    def test_put_get_parameter(self, make_boto_client):
        ssm = make_boto_client("ssm")
        name = f"/integ/test/{uuid.uuid4().hex[:8]}"

        ssm.put_parameter(Name=name, Value="test-value", Type="String")

        resp = ssm.get_parameter(Name=name)
        assert resp["Parameter"]["Value"] == "test-value"

        ssm.delete_parameter(Name=name)


class TestEventBridgeWithTargets:
    """EventBridge rules and targets."""

    def test_create_rule_with_sqs_target(self, make_boto_client):
        events = make_boto_client("events")
        sqs = make_boto_client("sqs")
        suffix = uuid.uuid4().hex[:8]

        # Create SQS queue as target
        q_resp = sqs.create_queue(QueueName=f"eb-target-{suffix}")
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        # Create EventBridge rule
        events.put_rule(
            Name=f"integ-rule-{suffix}",
            EventPattern=json.dumps({"source": ["integ.test"]}),
        )

        # Add SQS target
        events.put_targets(
            Rule=f"integ-rule-{suffix}",
            Targets=[{"Id": "sqs-target", "Arn": queue_arn}],
        )

        # Put event
        events.put_events(
            Entries=[
                {
                    "Source": "integ.test",
                    "DetailType": "TestEvent",
                    "Detail": json.dumps({"key": "value"}),
                }
            ]
        )

        # Allow time for delivery
        time.sleep(1)

        recv = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1

        # Cleanup
        events.remove_targets(Rule=f"integ-rule-{suffix}", Ids=["sqs-target"])
        events.delete_rule(Name=f"integ-rule-{suffix}")
        sqs.delete_queue(QueueUrl=queue_url)


class TestKinesisStream:
    """Kinesis stream operations."""

    def test_put_get_records(self, make_boto_client):
        kinesis = make_boto_client("kinesis")
        stream_name = f"integ-stream-{uuid.uuid4().hex[:8]}"

        kinesis.create_stream(StreamName=stream_name, ShardCount=1)

        kinesis.put_record(
            StreamName=stream_name,
            Data=b"test-data",
            PartitionKey="pk1",
        )

        desc = kinesis.describe_stream(StreamName=stream_name)
        shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]

        iter_resp = kinesis.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )

        records_resp = kinesis.get_records(ShardIterator=iter_resp["ShardIterator"])
        records = records_resp["Records"]
        assert len(records) >= 1
        assert records[0]["Data"] == b"test-data"

        kinesis.delete_stream(StreamName=stream_name)
