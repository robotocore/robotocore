"""CDK app: CodeBuild project + S3 artifact bucket + IAM role."""

import aws_cdk as cdk
from aws_cdk import aws_codebuild as codebuild
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3


class CicdPipelineStack(cdk.Stack):
    def __init__(self, scope, construct_id, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket(
            self,
            "ArtifactBucket",
            bucket_name="cicd-artifact-bucket",
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        role = iam.Role(
            self,
            "BuildRole",
            role_name="cicd-build-role",
            assumed_by=iam.ServicePrincipal("codebuild.amazonaws.com"),
        )
        bucket.grant_read_write(role)

        codebuild.Project(
            self,
            "BuildProject",
            project_name="cicd-build-project",
            role=role,
            source=codebuild.Source.s3(
                bucket=bucket,
                path="source/",
            ),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
            ),
            artifacts=codebuild.Artifacts.s3(
                bucket=bucket,
                path="artifacts/",
                include_build_id=False,
                package_zip=True,
            ),
        )

        cdk.CfnOutput(self, "BucketName", value=bucket.bucket_name)
        cdk.CfnOutput(self, "RoleName", value=role.role_name)


app = cdk.App()
CicdPipelineStack(app, "CicdPipelineStack")
app.synth()
