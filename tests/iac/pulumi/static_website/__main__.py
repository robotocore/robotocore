"""Pulumi program: S3 static website with public-read bucket policy."""

import json

import pulumi
import pulumi_aws as aws

bucket = aws.s3.Bucket(
    "website",
    website=aws.s3.BucketWebsiteArgs(
        index_document="index.html",
        error_document="error.html",
    ),
)

policy = aws.s3.BucketPolicy(
    "public-read",
    bucket=bucket.id,
    policy=bucket.arn.apply(
        lambda arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "PublicReadGetObject",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"{arn}/*",
                    }
                ],
            }
        )
    ),
)

pulumi.export("bucket_name", bucket.id)
pulumi.export("website_endpoint", bucket.website_endpoint)
