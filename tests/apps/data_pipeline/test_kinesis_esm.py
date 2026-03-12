"""Test Kinesis → Lambda ESM trigger for data pipeline.

Verifies that putting records to a Kinesis stream triggers Lambda
via an event source mapping.
"""

import json
import time

from tests.apps.conftest import make_lambda_zip


class TestKinesisEsm:
    """Kinesis stream → Lambda event source mapping."""

    def test_kinesis_records_trigger_lambda(
        self, kinesis, lambda_client, dynamodb, iam, unique_name
    ):
        """Put records to Kinesis stream → Lambda writes to DDB marker table."""
        stream_name = f"dp-stream-{unique_name}"
        table_name = f"dp-marker-{unique_name}"

        # Create Kinesis stream
        kinesis.create_stream(StreamName=stream_name, ShardCount=1)
        # Wait for ACTIVE
        deadline = time.time() + 15
        while time.time() < deadline:
            desc = kinesis.describe_stream(StreamName=stream_name)
            if desc["StreamDescription"]["StreamStatus"] == "ACTIVE":
                break
            time.sleep(1)
        stream_arn = desc["StreamDescription"]["StreamARN"]

        # Create marker DynamoDB table
        dynamodb.create_table(
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

        # Create IAM role
        role_resp = iam.create_role(
            RoleName=f"kin-role-{unique_name}",
            AssumeRolePolicyDocument=json.dumps(
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
            ),
        )
        role_arn = role_resp["Role"]["Arn"]

        endpoint_url = "http://localhost:4566"
        handler_code = f"""\
import json
import base64
import boto3

def handler(event, context):
    ddb = boto3.client("dynamodb", endpoint_url="{endpoint_url}")
    for record in event.get("Records", []):
        payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        data = json.loads(payload)
        ddb.put_item(
            TableName="{table_name}",
            Item={{
                "pk": {{"S": "kinesis-esm"}},
                "sk": {{"S": data.get("sensor_id", "unknown")}},
                "reading": {{"N": str(data.get("value", 0))}},
            }},
        )
    return {{"statusCode": 200}}
"""
        fn_name = f"kin-handler-{unique_name}"
        zip_bytes = make_lambda_zip(handler_code)
        lambda_client.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="index.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=30,
        )

        # Create ESM
        esm_resp = lambda_client.create_event_source_mapping(
            EventSourceArn=stream_arn,
            FunctionName=fn_name,
            StartingPosition="LATEST",
            BatchSize=1,
            Enabled=True,
        )
        esm_uuid = esm_resp["UUID"]

        # Put a record
        kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps({"sensor_id": "TEMP-01", "value": 23.5}).encode(),
            PartitionKey="sensor-1",
        )

        # Wait for Lambda to process
        deadline = time.time() + 15
        item = None
        while time.time() < deadline:
            resp = dynamodb.get_item(
                TableName=table_name,
                Key={"pk": {"S": "kinesis-esm"}, "sk": {"S": "TEMP-01"}},
            )
            item = resp.get("Item")
            if item:
                break
            time.sleep(1)

        assert item is not None, "Lambda did not process Kinesis record within 15s"
        assert float(item["reading"]["N"]) == 23.5

        # Cleanup
        lambda_client.delete_event_source_mapping(UUID=esm_uuid)
        lambda_client.delete_function(FunctionName=fn_name)
        kinesis.delete_stream(StreamName=stream_name)
        dynamodb.delete_table(TableName=table_name)
        iam.delete_role(RoleName=f"kin-role-{unique_name}")
