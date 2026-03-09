"""Pulumi program: Data lake with S3 landing zone, Kinesis stream, and DynamoDB catalog."""

import pulumi
import pulumi_aws as aws

# S3 landing zone bucket
landing_bucket = aws.s3.Bucket("landing-zone")

# Kinesis ingest stream (1 shard)
ingest_stream = aws.kinesis.Stream(
    "ingest-stream",
    shard_count=1,
)

# DynamoDB catalog table
catalog_table = aws.dynamodb.Table(
    "catalog",
    attributes=[
        aws.dynamodb.TableAttributeArgs(name="dataset_id", type="S"),
        aws.dynamodb.TableAttributeArgs(name="timestamp", type="S"),
    ],
    hash_key="dataset_id",
    range_key="timestamp",
    billing_mode="PAY_PER_REQUEST",
)

pulumi.export("bucket_name", landing_bucket.id)
pulumi.export("stream_name", ingest_stream.name)
pulumi.export("table_name", catalog_table.name)
