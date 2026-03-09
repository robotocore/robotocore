"""Pulumi program: Cognito User Pool + App Client."""

import pulumi
import pulumi_aws as aws

user_pool = aws.cognito.UserPool(
    "auth-user-pool",
    name="auth-user-pool",
    password_policy=aws.cognito.UserPoolPasswordPolicyArgs(
        minimum_length=8,
        require_lowercase=True,
        require_numbers=True,
        require_symbols=False,
        require_uppercase=True,
    ),
    auto_verified_attributes=["email"],
)

app_client = aws.cognito.UserPoolClient(
    "auth-app-client",
    name="auth-app-client",
    user_pool_id=user_pool.id,
    explicit_auth_flows=[
        "ALLOW_USER_PASSWORD_AUTH",
        "ALLOW_REFRESH_TOKEN_AUTH",
    ],
    generate_secret=False,
)

pulumi.export("user_pool_id", user_pool.id)
pulumi.export("app_client_id", app_client.id)
