"""Pulumi program: S3 artifact bucket + IAM role for CI/CD."""

import json

import pulumi
import pulumi_aws as aws

artifact_bucket = aws.s3.Bucket(
    "cicd-artifacts",
    bucket="cicd-artifacts-bucket",
    force_destroy=True,
)

assume_role_policy = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "codepipeline.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
)

pipeline_role = aws.iam.Role(
    "cicd-pipeline-role",
    name="cicd-pipeline-role",
    assume_role_policy=assume_role_policy,
)

s3_policy = aws.iam.RolePolicy(
    "cicd-s3-policy",
    name="cicd-s3-policy",
    role=pipeline_role.id,
    policy=artifact_bucket.arn.apply(
        lambda arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:PutObject"],
                        "Resource": f"{arn}/*",
                    }
                ],
            }
        )
    ),
)

pulumi.export("bucket_name", artifact_bucket.bucket)
pulumi.export("role_arn", pipeline_role.arn)
pulumi.export("role_name", pipeline_role.name)
