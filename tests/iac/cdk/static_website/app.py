#!/usr/bin/env python3
"""CDK app: Static website with S3 bucket hosting and public-read policy."""

import aws_cdk as cdk
from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from constructs import Construct


class StaticWebsiteStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 bucket with static website hosting enabled
        bucket = s3.Bucket(
            self,
            "WebsiteBucket",
            website_index_document="index.html",
            website_error_document="error.html",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=False,
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=False,
                block_public_policy=False,
                ignore_public_acls=False,
                restrict_public_buckets=False,
            ),
        )

        # Bucket policy allowing public read access
        bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[bucket.arn_for_objects("*")],
                principals=[iam.AnyPrincipal()],
            )
        )

        CfnOutput(self, "BucketName", value=bucket.bucket_name)
        CfnOutput(self, "WebsiteURL", value=bucket.bucket_website_url)


app = cdk.App()
StaticWebsiteStack(app, "StaticWebsite")
app.synth()
