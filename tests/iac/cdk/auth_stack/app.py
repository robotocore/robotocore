"""CDK app: Cognito User Pool + App Client + IAM role."""

import aws_cdk as cdk
from aws_cdk import aws_cognito as cognito
from aws_cdk import aws_iam as iam


class AuthStack(cdk.Stack):
    def __init__(self, scope, construct_id, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        pool = cognito.UserPool(
            self,
            "UserPool",
            user_pool_name="auth-userpool",
            self_sign_up_enabled=True,
            auto_verify=cognito.AutoVerifiedAttrs(email=True),
            password_policy=cognito.PasswordPolicy(
                min_length=12,
                require_lowercase=True,
                require_uppercase=True,
                require_digits=True,
                require_symbols=True,
            ),
        )

        client = pool.add_client(
            "AppClient",
            user_pool_client_name="auth-client",
            auth_flows=cognito.AuthFlow(
                user_password=True,
                user_srp=True,
            ),
        )

        role = iam.Role(
            self,
            "AuthRole",
            role_name="auth-role",
            assumed_by=iam.ServicePrincipal("cognito-idp.amazonaws.com"),
        )

        cdk.CfnOutput(self, "UserPoolId", value=pool.user_pool_id)
        cdk.CfnOutput(self, "UserPoolClientId", value=client.user_pool_client_id)
        cdk.CfnOutput(self, "RoleName", value=role.role_name)


app = cdk.App()
AuthStack(app, "AuthStack")
app.synth()
