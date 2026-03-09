#!/usr/bin/env python3
"""CDK app: Data lake with S3 landing zone, Kinesis ingest stream, and DynamoDB catalog."""

import aws_cdk as cdk
from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_kinesis as kinesis
from aws_cdk import aws_s3 as s3
from constructs import Construct


class DataLakeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 landing zone bucket
        landing_bucket = s3.Bucket(
            self,
            "LandingZone",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=False,
        )

        # Kinesis ingest stream (1 shard)
        stream = kinesis.Stream(
            self,
            "IngestStream",
            stream_name="data-lake-ingest",
            shard_count=1,
        )

        # DynamoDB catalog table
        catalog_table = dynamodb.Table(
            self,
            "CatalogTable",
            table_name="data-lake-catalog",
            partition_key=dynamodb.Attribute(name="dataset_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="timestamp", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY,
        )

        CfnOutput(self, "LandingBucketName", value=landing_bucket.bucket_name)
        CfnOutput(self, "IngestStreamName", value=stream.stream_name)
        CfnOutput(self, "CatalogTableName", value=catalog_table.table_name)


app = cdk.App()
DataLakeStack(app, "DataLake")
app.synth()
