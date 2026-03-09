"""STS compatibility tests."""

import json
import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def sts():
    return make_client("sts")


class TestSTSOperations:
    def test_get_caller_identity(self, sts):
        response = sts.get_caller_identity()
        assert "Account" in response
        assert "Arn" in response
        assert "UserId" in response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_session_token(self, sts):
        response = sts.get_session_token()
        creds = response["Credentials"]
        assert "AccessKeyId" in creds
        assert "SecretAccessKey" in creds
        assert "SessionToken" in creds

    def test_assume_role(self, sts):
        import uuid

        role_name = f"test-sts-role-{uuid.uuid4().hex[:8]}"
        iam = make_client("iam")
        trust_policy = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Allow","Principal":{"AWS":"*"},'
            '"Action":"sts:AssumeRole"}]}'
        )
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=trust_policy,
        )
        role_arn = role["Role"]["Arn"]
        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="test-session",
        )
        assert "Credentials" in response
        assert "AssumedRoleUser" in response
        iam.delete_role(RoleName=role_name)

    def test_assume_role_session_credentials(self, sts):
        """Verify assumed-role credentials contain all required fields."""
        iam = make_client("iam")
        trust_policy = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Allow","Principal":{"AWS":"*"},'
            '"Action":"sts:AssumeRole"}]}'
        )
        role = iam.create_role(
            RoleName="test-creds-role",
            AssumeRolePolicyDocument=trust_policy,
        )
        role_arn = role["Role"]["Arn"]
        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="creds-session",
        )
        creds = response["Credentials"]
        assert "AccessKeyId" in creds
        assert "SecretAccessKey" in creds
        assert "SessionToken" in creds
        assert "Expiration" in creds
        iam.delete_role(RoleName="test-creds-role")

    def test_get_access_key_info(self, sts):
        """Get account info for an access key."""
        response = sts.get_access_key_info(AccessKeyId="AKIAIOSFODNN7EXAMPLE")
        assert "Account" in response

    def test_assume_role_with_tags(self, sts):
        """Assume role with session tags."""
        import uuid

        role_name = f"test-tags-role-{uuid.uuid4().hex[:8]}"
        iam = make_client("iam")
        trust_policy = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Allow","Principal":{"AWS":"*"},'
            '"Action":"sts:AssumeRole"}]}'
        )
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=trust_policy,
        )
        role_arn = role["Role"]["Arn"]
        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="tag-session",
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        assert "Credentials" in response
        assert "AssumedRoleUser" in response
        iam.delete_role(RoleName=role_name)

    def test_get_federation_token(self, sts):
        response = sts.get_federation_token(Name="testuser")
        creds = response["Credentials"]
        assert "AccessKeyId" in creds
        assert "SecretAccessKey" in creds
        assert "SessionToken" in creds
        assert "FederatedUser" in response
        assert "FederatedUserId" in response["FederatedUser"]

    def test_decode_authorization_message(self, sts):
        """Call decode_authorization_message with a dummy encoded message."""
        try:
            sts.decode_authorization_message(EncodedMessage="dummy-encoded-message")
        except sts.exceptions.InvalidAuthorizationMessageException:
            # Expected — the message is not a real encoded authorization message
            pass

    def test_assume_role_with_duration(self, sts):
        """AssumeRole with DurationSeconds."""
        import uuid

        iam = make_client("iam")
        role_name = f"dur-role-{uuid.uuid4().hex[:8]}"
        trust_policy = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Allow","Principal":{"AWS":"*"},'
            '"Action":"sts:AssumeRole"}]}'
        )
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=trust_policy,
        )
        role_arn = role["Role"]["Arn"]
        try:
            response = sts.assume_role(
                RoleArn=role_arn,
                RoleSessionName="dur-session",
                DurationSeconds=3600,
            )
            assert "Credentials" in response
            creds = response["Credentials"]
            assert "AccessKeyId" in creds
            assert "Expiration" in creds
        finally:
            iam.delete_role(RoleName=role_name)

    def test_assume_role_with_inline_policy(self, sts):
        """AssumeRole with an inline session Policy."""
        import json
        import uuid

        iam = make_client("iam")
        role_name = f"pol-role-{uuid.uuid4().hex[:8]}"
        trust_policy = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Allow","Principal":{"AWS":"*"},'
            '"Action":"sts:AssumeRole"}]}'
        )
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=trust_policy,
        )
        role_arn = role["Role"]["Arn"]
        session_policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "s3:GetObject",
                        "Resource": "arn:aws:s3:::my-bucket/*",
                    }
                ],
            }
        )
        try:
            response = sts.assume_role(
                RoleArn=role_arn,
                RoleSessionName="policy-session",
                Policy=session_policy,
            )
            assert "Credentials" in response
            assert "AssumedRoleUser" in response
        finally:
            iam.delete_role(RoleName=role_name)

    def test_get_session_token_with_duration(self, sts):
        """GetSessionToken with DurationSeconds."""
        response = sts.get_session_token(DurationSeconds=900)
        creds = response["Credentials"]
        assert "AccessKeyId" in creds
        assert "SecretAccessKey" in creds
        assert "SessionToken" in creds
        assert "Expiration" in creds

    def test_caller_identity_account_format(self, sts):
        """GetCallerIdentity returns 12-digit account ID."""
        response = sts.get_caller_identity()
        assert len(response["Account"]) == 12
        assert response["Account"].isdigit()

    def test_caller_identity_arn_format(self, sts):
        """GetCallerIdentity ARN starts with arn:aws."""
        response = sts.get_caller_identity()
        assert response["Arn"].startswith("arn:aws:")

    def test_assume_role_assumed_role_user_fields(self, sts):
        """AssumedRoleUser has Arn and AssumedRoleId."""
        iam = make_client("iam")
        role_name = f"aru-role-{uuid.uuid4().hex[:8]}"
        trust = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole"}
                ],
            }
        )
        role = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust)
        try:
            resp = sts.assume_role(RoleArn=role["Role"]["Arn"], RoleSessionName="aru-session")
            aru = resp["AssumedRoleUser"]
            assert "Arn" in aru
            assert "AssumedRoleId" in aru
            assert "aru-session" in aru["Arn"]
        finally:
            iam.delete_role(RoleName=role_name)

    def test_get_federation_token_with_policy(self, sts):
        """GetFederationToken with an inline Policy."""
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
            }
        )
        response = sts.get_federation_token(Name="poluser", Policy=policy)
        assert "Credentials" in response
        assert "FederatedUser" in response

    def test_get_federation_token_credentials_fields(self, sts):
        """GetFederationToken credentials have all required fields."""
        response = sts.get_federation_token(Name="fielduser")
        creds = response["Credentials"]
        assert "AccessKeyId" in creds
        assert "SecretAccessKey" in creds
        assert "SessionToken" in creds
        assert "Expiration" in creds


class TestSTSExtended:
    """Extended STS compatibility tests covering additional scenarios."""

    TRUST_POLICY = (
        '{"Version":"2012-10-17","Statement":'
        '[{"Effect":"Allow","Principal":{"AWS":"*"},'
        '"Action":"sts:AssumeRole"}]}'
    )

    def _create_role(self, iam, role_name):
        """Helper to create a role and return its ARN."""
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=self.TRUST_POLICY,
        )
        return role["Role"]["Arn"]

    def test_get_caller_identity_account_format(self, sts):
        """Verify Account is a 12-digit string."""
        response = sts.get_caller_identity()
        account = response["Account"]
        assert len(account) == 12
        assert account.isdigit()

    def test_get_caller_identity_arn_format(self, sts):
        """Verify Arn follows the expected ARN pattern."""
        response = sts.get_caller_identity()
        arn = response["Arn"]
        assert arn.startswith("arn:aws:")

    def test_get_caller_identity_userid_present(self, sts):
        """Verify UserId is a non-empty string."""
        response = sts.get_caller_identity()
        assert isinstance(response["UserId"], str)
        assert len(response["UserId"]) > 0

    def test_assume_role_with_external_id(self, sts):
        """AssumeRole with ExternalId parameter."""
        import uuid

        iam = make_client("iam")
        role_name = f"extid-role-{uuid.uuid4().hex[:8]}"
        role_arn = self._create_role(iam, role_name)
        try:
            response = sts.assume_role(
                RoleArn=role_arn,
                RoleSessionName="extid-session",
                ExternalId="my-external-id-12345",
            )
            assert "Credentials" in response
            assert "AssumedRoleUser" in response
        finally:
            iam.delete_role(RoleName=role_name)

    def test_assume_role_assumed_role_user_arn(self, sts):
        """Verify AssumedRoleUser.Arn contains session name."""
        import uuid

        iam = make_client("iam")
        role_name = f"arn-role-{uuid.uuid4().hex[:8]}"
        role_arn = self._create_role(iam, role_name)
        try:
            response = sts.assume_role(
                RoleArn=role_arn,
                RoleSessionName="my-unique-session",
            )
            assumed_arn = response["AssumedRoleUser"]["Arn"]
            assert "my-unique-session" in assumed_arn
            assert "assumed-role" in assumed_arn
        finally:
            iam.delete_role(RoleName=role_name)

    def test_assume_role_assumed_role_user_id(self, sts):
        """Verify AssumedRoleUser.AssumedRoleId contains session name."""
        import uuid

        iam = make_client("iam")
        role_name = f"arid-role-{uuid.uuid4().hex[:8]}"
        role_arn = self._create_role(iam, role_name)
        try:
            response = sts.assume_role(
                RoleArn=role_arn,
                RoleSessionName="session-id-check",
            )
            assumed_role_id = response["AssumedRoleUser"]["AssumedRoleId"]
            assert "session-id-check" in assumed_role_id
        finally:
            iam.delete_role(RoleName=role_name)

    def test_assume_role_with_transitive_tag_keys(self, sts):
        """AssumeRole with Tags and TransitiveTagKeys."""
        import uuid

        iam = make_client("iam")
        role_name = f"ttk-role-{uuid.uuid4().hex[:8]}"
        role_arn = self._create_role(iam, role_name)
        try:
            response = sts.assume_role(
                RoleArn=role_arn,
                RoleSessionName="ttk-session",
                Tags=[
                    {"Key": "Project", "Value": "robotocore"},
                    {"Key": "Department", "Value": "eng"},
                ],
                TransitiveTagKeys=["Project"],
            )
            assert "Credentials" in response
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            iam.delete_role(RoleName=role_name)

    def test_assume_role_with_policy_arns(self, sts):
        """AssumeRole with managed PolicyArns for session."""
        import uuid

        iam = make_client("iam")
        role_name = f"parns-role-{uuid.uuid4().hex[:8]}"
        role_arn = self._create_role(iam, role_name)
        try:
            response = sts.assume_role(
                RoleArn=role_arn,
                RoleSessionName="parns-session",
                PolicyArns=[
                    {"arn": "arn:aws:iam::aws:policy/ReadOnlyAccess"},
                ],
            )
            assert "Credentials" in response
            assert "AssumedRoleUser" in response
        finally:
            iam.delete_role(RoleName=role_name)

    def test_assume_role_credential_access_key_format(self, sts):
        """Verify AccessKeyId from assume_role starts with ASIA."""
        import uuid

        iam = make_client("iam")
        role_name = f"akfmt-role-{uuid.uuid4().hex[:8]}"
        role_arn = self._create_role(iam, role_name)
        try:
            response = sts.assume_role(
                RoleArn=role_arn,
                RoleSessionName="akfmt-session",
            )
            access_key = response["Credentials"]["AccessKeyId"]
            assert access_key.startswith("ASIA"), f"Expected ASIA prefix, got {access_key[:4]}"
        finally:
            iam.delete_role(RoleName=role_name)

    def test_assume_role_expiration_is_datetime(self, sts):
        """Verify Expiration is a datetime object (parsed by botocore)."""
        import datetime
        import uuid

        iam = make_client("iam")
        role_name = f"expdt-role-{uuid.uuid4().hex[:8]}"
        role_arn = self._create_role(iam, role_name)
        try:
            response = sts.assume_role(
                RoleArn=role_arn,
                RoleSessionName="expdt-session",
            )
            expiration = response["Credentials"]["Expiration"]
            assert isinstance(expiration, datetime.datetime)
        finally:
            iam.delete_role(RoleName=role_name)

    def test_get_session_token_credential_format(self, sts):
        """Verify GetSessionToken returns properly formatted temporary credentials."""
        response = sts.get_session_token()
        creds = response["Credentials"]
        # SessionToken should be a non-empty string
        assert isinstance(creds["SessionToken"], str)
        assert len(creds["SessionToken"]) > 0
        # AccessKeyId should be a non-empty string
        assert isinstance(creds["AccessKeyId"], str)
        assert len(creds["AccessKeyId"]) > 0

    def test_get_session_token_expiration_is_datetime(self, sts):
        """Verify GetSessionToken Expiration is a datetime."""
        import datetime

        response = sts.get_session_token()
        expiration = response["Credentials"]["Expiration"]
        assert isinstance(expiration, datetime.datetime)

    def test_get_federation_token_with_policy(self, sts):
        """GetFederationToken with an inline policy."""
        import json
        import uuid

        name = f"feduser-{uuid.uuid4().hex[:8]}"
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "s3:GetObject",
                        "Resource": "*",
                    }
                ],
            }
        )
        response = sts.get_federation_token(Name=name, Policy=policy)
        assert "Credentials" in response
        assert "FederatedUser" in response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_federation_token_with_duration(self, sts):
        """GetFederationToken with DurationSeconds."""
        import uuid

        name = f"feduser-{uuid.uuid4().hex[:8]}"
        response = sts.get_federation_token(
            Name=name,
            DurationSeconds=900,
        )
        creds = response["Credentials"]
        assert "AccessKeyId" in creds
        assert "Expiration" in creds

    def test_get_federation_token_federated_user_arn(self, sts):
        """Verify FederatedUser.Arn contains the user name."""
        import uuid

        name = f"fedarn-{uuid.uuid4().hex[:8]}"
        response = sts.get_federation_token(Name=name)
        fed_arn = response["FederatedUser"]["Arn"]
        assert name in fed_arn
        assert "federated-user" in fed_arn

    def test_assume_role_packed_policy_too_large(self, sts):
        """AssumeRole with an excessively large Policy should fail."""
        import uuid

        iam = make_client("iam")
        role_name = f"bigpol-role-{uuid.uuid4().hex[:8]}"
        role_arn = self._create_role(iam, role_name)
        try:
            # Build a policy that exceeds the 2048-character packed limit
            huge_policy = (
                '{"Version":"2012-10-17","Statement":['
                + ",".join(
                    [
                        '{"Effect":"Allow","Action":"s3:GetObject",'
                        f'"Resource":"arn:aws:s3:::bucket-{i}-' + "x" * 200 + '/*"}'
                        for i in range(20)
                    ]
                )
                + "]}"
            )
            with pytest.raises(Exception):
                sts.assume_role(
                    RoleArn=role_arn,
                    RoleSessionName="bigpol-session",
                    Policy=huge_policy,
                )
        finally:
            iam.delete_role(RoleName=role_name)

    def test_assume_role_with_web_identity(self, sts):
        """AssumeRoleWithWebIdentity with a dummy token."""
        import uuid

        iam = make_client("iam")
        role_name = f"webid-role-{uuid.uuid4().hex[:8]}"
        role_arn = self._create_role(iam, role_name)
        try:
            response = sts.assume_role_with_web_identity(
                RoleArn=role_arn,
                RoleSessionName="webid-session",
                WebIdentityToken="dummy-oidc-token",
            )
            assert "Credentials" in response
        finally:
            iam.delete_role(RoleName=role_name)

    def test_assume_role_with_saml(self, sts):
        """AssumeRoleWithSAML with dummy parameters."""
        import uuid

        iam = make_client("iam")
        role_name = f"saml-role-{uuid.uuid4().hex[:8]}"
        role_arn = self._create_role(iam, role_name)
        try:
            response = sts.assume_role_with_saml(
                RoleArn=role_arn,
                PrincipalArn="arn:aws:iam::123456789012:saml-provider/MyProvider",
                SAMLAssertion="PHNhbWw+ZHVtbXk8L3NhbWw+",
            )
            assert "Credentials" in response
        finally:
            iam.delete_role(RoleName=role_name)

    def test_assume_role_with_saml_credential_fields(self, sts):
        """AssumeRoleWithSAML returns all required credential fields."""
        import datetime
        import uuid

        iam = make_client("iam")
        role_name = f"samlcf-role-{uuid.uuid4().hex[:8]}"
        role_arn = self._create_role(iam, role_name)
        try:
            response = sts.assume_role_with_saml(
                RoleArn=role_arn,
                PrincipalArn="arn:aws:iam::123456789012:saml-provider/MyProvider",
                SAMLAssertion="PHNhbWw+ZHVtbXk8L3NhbWw+",
            )
            creds = response["Credentials"]
            assert "AccessKeyId" in creds
            assert "SecretAccessKey" in creds
            assert "SessionToken" in creds
            assert "Expiration" in creds
            assert isinstance(creds["Expiration"], datetime.datetime)
        finally:
            iam.delete_role(RoleName=role_name)

    def test_assume_role_with_saml_assumed_role_user(self, sts):
        """AssumeRoleWithSAML returns AssumedRoleUser with Arn and AssumedRoleId."""
        import uuid

        iam = make_client("iam")
        role_name = f"samlaru-role-{uuid.uuid4().hex[:8]}"
        role_arn = self._create_role(iam, role_name)
        try:
            response = sts.assume_role_with_saml(
                RoleArn=role_arn,
                PrincipalArn="arn:aws:iam::123456789012:saml-provider/MyProvider",
                SAMLAssertion="PHNhbWw+ZHVtbXk8L3NhbWw+",
            )
            assert "AssumedRoleUser" in response
            aru = response["AssumedRoleUser"]
            assert "Arn" in aru
            assert "AssumedRoleId" in aru
        finally:
            iam.delete_role(RoleName=role_name)

    def test_assume_role_with_saml_with_policy(self, sts):
        """AssumeRoleWithSAML with an inline session policy."""
        import uuid

        iam = make_client("iam")
        role_name = f"samlpol-role-{uuid.uuid4().hex[:8]}"
        role_arn = self._create_role(iam, role_name)
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}],
            }
        )
        try:
            response = sts.assume_role_with_saml(
                RoleArn=role_arn,
                PrincipalArn="arn:aws:iam::123456789012:saml-provider/MyProvider",
                SAMLAssertion="PHNhbWw+ZHVtbXk8L3NhbWw+",
                Policy=policy,
            )
            assert "Credentials" in response
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            iam.delete_role(RoleName=role_name)

    def test_assume_root(self, sts):
        """AssumeRoot returns temporary credentials for a target principal."""
        response = sts.assume_root(
            TargetPrincipal="arn:aws:organizations::123456789012:account/o-test/123456789012",
            TaskPolicyArn={"arn": "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials"},
        )
        assert "Credentials" in response
        creds = response["Credentials"]
        assert "AccessKeyId" in creds
        assert "SecretAccessKey" in creds
        assert "SessionToken" in creds

    def test_get_delegated_access_token(self, sts):
        """GetDelegatedAccessToken returns temporary credentials."""
        response = sts.get_delegated_access_token(TradeInToken="dummy-trade-in-token")
        assert "Credentials" in response
        creds = response["Credentials"]
        assert "AccessKeyId" in creds
        assert "SecretAccessKey" in creds
        assert "SessionToken" in creds
        assert "Expiration" in creds

    def test_assume_role_with_saml_with_duration(self, sts):
        """AssumeRoleWithSAML with DurationSeconds."""
        import uuid

        iam = make_client("iam")
        role_name = f"samldur-role-{uuid.uuid4().hex[:8]}"
        role_arn = self._create_role(iam, role_name)
        try:
            response = sts.assume_role_with_saml(
                RoleArn=role_arn,
                PrincipalArn="arn:aws:iam::123456789012:saml-provider/MyProvider",
                SAMLAssertion="PHNhbWw+ZHVtbXk8L3NhbWw+",
                DurationSeconds=3600,
            )
            assert "Credentials" in response
            assert "AccessKeyId" in response["Credentials"]
        finally:
            iam.delete_role(RoleName=role_name)


class TestSTSAdditionalOps:
    """Additional STS operations."""

    @pytest.fixture
    def client(self):
        return make_client("sts")

    def test_assume_role_with_saml_minimal(self, client):
        """AssumeRoleWithSAML with minimal fake assertion returns credentials."""
        iam = make_client("iam")
        role_name = f"saml-add-{uuid.uuid4().hex[:8]}"
        trust_policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Federated": "*"},
                        "Action": "sts:AssumeRoleWithSAML",
                    }
                ],
            }
        )
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=trust_policy,
        )
        role_arn = role["Role"]["Arn"]
        try:
            resp = client.assume_role_with_saml(
                RoleArn=role_arn,
                PrincipalArn="arn:aws:iam::123456789012:saml-provider/TestProv",
                SAMLAssertion="PHNhbWw+ZHVtbXk8L3NhbWw+",
            )
            assert "Credentials" in resp
            assert "SecretAccessKey" in resp["Credentials"]
            assert "SessionToken" in resp["Credentials"]
        finally:
            iam.delete_role(RoleName=role_name)
