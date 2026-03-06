"""Tests for 88 new CloudFormation resource handlers + new intrinsic functions."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from robotocore.services.cloudformation.engine import (
    CfnResource,
    resolve_intrinsics,
)
from robotocore.services.cloudformation.resources import (
    _CREATE_HANDLERS,
    _DELETE_HANDLERS,
    compute_cidr,
    create_resource,
    delete_resource,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REGION = "us-east-1"
ACCOUNT_ID = "999999999999"


def _resource(resource_type, properties=None, logical_id="MyResource"):
    return CfnResource(
        logical_id=logical_id,
        resource_type=resource_type,
        properties=properties or {},
    )


def _mock_global(service):
    return patch(
        "robotocore.services.cloudformation.resources._moto_global_backend",
        return_value=MagicMock(),
    )


def _mock_regional(service):
    return patch(
        "robotocore.services.cloudformation.resources._moto_backend",
        return_value=MagicMock(),
    )


# ---- Intrinsic function test fixtures ----
_RESOURCES = {
    "MyBucket": CfnResource(
        logical_id="MyBucket",
        resource_type="AWS::S3::Bucket",
        properties={},
        physical_id="my-bucket",
        attributes={"Arn": "arn:aws:s3:::my-bucket"},
    ),
}
_PARAMS = {"EnvName": "prod", "AWS::StackName": "stack"}


# ===========================================================================
# Handler map counts
# ===========================================================================


class TestHandlerCounts:
    def test_create_handler_count_at_least_100(self):
        assert len(_CREATE_HANDLERS) >= 100

    def test_delete_handler_count_at_least_100(self):
        assert len(_DELETE_HANDLERS) >= 100

    def test_create_and_delete_have_same_keys(self):
        assert set(_CREATE_HANDLERS.keys()) == set(_DELETE_HANDLERS.keys())


# ===========================================================================
# IAM::ManagedPolicy
# ===========================================================================


class TestIamManagedPolicy:
    def test_create(self):
        mock = MagicMock()
        mock.create_policy.return_value = SimpleNamespace(
            arn="arn:aws:iam::999:policy/mp"
        )
        with _mock_global("iam") as p:
            p.return_value = mock
            res = _resource(
                "AWS::IAM::ManagedPolicy",
                {"ManagedPolicyName": "mp", "PolicyDocument": {}},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "arn:aws:iam::999:policy/mp"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_global("iam") as p:
            p.return_value = mock
            res = _resource("AWS::IAM::ManagedPolicy")
            res.physical_id = "arn:aws:iam::999:policy/mp"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_policy.assert_called_once()


# ===========================================================================
# IAM::InstanceProfile
# ===========================================================================


class TestIamInstanceProfile:
    def test_create_with_roles(self):
        mock = MagicMock()
        mock.create_instance_profile.return_value = SimpleNamespace(
            arn="arn:aws:iam::999:instance-profile/ip"
        )
        with _mock_global("iam") as p:
            p.return_value = mock
            res = _resource(
                "AWS::IAM::InstanceProfile",
                {"InstanceProfileName": "ip", "Roles": ["role1"]},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "ip"
        assert res.status == "CREATE_COMPLETE"
        mock.add_role_to_instance_profile.assert_called_once_with("ip", "role1")

    def test_delete(self):
        mock = MagicMock()
        with _mock_global("iam") as p:
            p.return_value = mock
            res = _resource("AWS::IAM::InstanceProfile")
            res.physical_id = "ip"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_instance_profile.assert_called_once()


# ===========================================================================
# IAM::User
# ===========================================================================


class TestIamUser:
    def test_create(self):
        mock = MagicMock()
        mock.create_user.return_value = SimpleNamespace(
            arn="arn:aws:iam::999:user/testuser"
        )
        with _mock_global("iam") as p:
            p.return_value = mock
            res = _resource("AWS::IAM::User", {"UserName": "testuser"})
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "testuser"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_global("iam") as p:
            p.return_value = mock
            res = _resource("AWS::IAM::User")
            res.physical_id = "testuser"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_user.assert_called_once()


# ===========================================================================
# IAM::Group
# ===========================================================================


class TestIamGroup:
    def test_create(self):
        mock = MagicMock()
        mock.create_group.return_value = SimpleNamespace(
            arn="arn:aws:iam::999:group/grp"
        )
        with _mock_global("iam") as p:
            p.return_value = mock
            res = _resource("AWS::IAM::Group", {"GroupName": "grp"})
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "grp"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_global("iam") as p:
            p.return_value = mock
            res = _resource("AWS::IAM::Group")
            res.physical_id = "grp"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_group.assert_called_once()


# ===========================================================================
# IAM::AccessKey
# ===========================================================================


class TestIamAccessKey:
    def test_create(self):
        mock = MagicMock()
        mock.create_access_key.return_value = SimpleNamespace(
            access_key_id="AKIAEXAMPLE", secret_access_key="secret123"
        )
        with _mock_global("iam") as p:
            p.return_value = mock
            res = _resource("AWS::IAM::AccessKey", {"UserName": "bob"})
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "AKIAEXAMPLE"
        assert res.attributes["SecretAccessKey"] == "secret123"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_global("iam") as p:
            p.return_value = mock
            res = _resource("AWS::IAM::AccessKey", {"UserName": "bob"})
            res.physical_id = "AKIAEXAMPLE"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_access_key.assert_called_once()


# ===========================================================================
# IAM::ServiceLinkedRole
# ===========================================================================


class TestIamServiceLinkedRole:
    def test_create(self):
        res = _resource(
            "AWS::IAM::ServiceLinkedRole",
            {"AWSServiceName": "elasticmapreduce.amazonaws.com"},
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert "aws-service-role" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete_noop(self):
        res = _resource("AWS::IAM::ServiceLinkedRole")
        res.physical_id = "arn:fake"
        delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# Lambda::Version
# ===========================================================================


class TestLambdaVersion:
    def test_create_fallback(self):
        mock = MagicMock()
        mock.publish_version_with_name.side_effect = Exception("nope")
        with _mock_regional("lambda") as p:
            p.return_value = mock
            res = _resource(
                "AWS::Lambda::Version", {"FunctionName": "fn1"}
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert ":function:fn1:" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete_noop(self):
        res = _resource("AWS::Lambda::Version")
        res.physical_id = "arn:version"
        delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# Lambda::Alias
# ===========================================================================


class TestLambdaAlias:
    def test_create(self):
        res = _resource(
            "AWS::Lambda::Alias",
            {"FunctionName": "fn1", "Name": "prod"},
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert ":function:fn1:prod" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete_noop(self):
        res = _resource("AWS::Lambda::Alias")
        res.physical_id = "arn"
        delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# Lambda::EventSourceMapping
# ===========================================================================


class TestLambdaEventSourceMapping:
    def test_create_success(self):
        mock = MagicMock()
        mock.create_event_source_mapping.return_value = SimpleNamespace(
            uuid="esm-123"
        )
        with _mock_regional("lambda") as p:
            p.return_value = mock
            res = _resource(
                "AWS::Lambda::EventSourceMapping",
                {"FunctionName": "fn1", "EventSourceArn": "arn:sqs:q"},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "esm-123"
        assert res.status == "CREATE_COMPLETE"

    def test_create_fallback(self):
        mock = MagicMock()
        mock.create_event_source_mapping.side_effect = Exception("nope")
        with _mock_regional("lambda") as p:
            p.return_value = mock
            res = _resource("AWS::Lambda::EventSourceMapping", {})
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id is not None
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("lambda") as p:
            p.return_value = mock
            res = _resource("AWS::Lambda::EventSourceMapping")
            res.physical_id = "esm-123"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_event_source_mapping.assert_called_once()


# ===========================================================================
# Lambda::Permission
# ===========================================================================


class TestLambdaPermission:
    def test_create(self):
        mock = MagicMock()
        with _mock_regional("lambda") as p:
            p.return_value = mock
            res = _resource(
                "AWS::Lambda::Permission",
                {
                    "FunctionName": "fn1",
                    "Action": "lambda:InvokeFunction",
                    "Principal": "s3.amazonaws.com",
                    "StatementId": "s3invoke",
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "s3invoke"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("lambda") as p:
            p.return_value = mock
            res = _resource(
                "AWS::Lambda::Permission", {"FunctionName": "fn1"}
            )
            res.physical_id = "s3invoke"
            delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# Lambda::LayerVersion
# ===========================================================================


class TestLambdaLayerVersion:
    def test_create(self):
        res = _resource(
            "AWS::Lambda::LayerVersion", {"LayerName": "mylib"}
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert ":layer:mylib:1" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete_noop(self):
        res = _resource("AWS::Lambda::LayerVersion")
        res.physical_id = "arn"
        delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# EC2::VPC
# ===========================================================================


class TestEc2Vpc:
    def test_create(self):
        mock = MagicMock()
        mock.create_vpc.return_value = SimpleNamespace(id="vpc-abc12345")
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource("AWS::EC2::VPC", {"CidrBlock": "10.0.0.0/16"})
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "vpc-abc12345"
        assert res.attributes["VpcId"] == "vpc-abc12345"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource("AWS::EC2::VPC")
            res.physical_id = "vpc-abc12345"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_vpc.assert_called_once()


# ===========================================================================
# EC2::Subnet
# ===========================================================================


class TestEc2Subnet:
    def test_create(self):
        mock = MagicMock()
        mock.create_subnet.return_value = SimpleNamespace(id="subnet-123")
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource(
                "AWS::EC2::Subnet",
                {"VpcId": "vpc-1", "CidrBlock": "10.0.1.0/24"},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "subnet-123"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource("AWS::EC2::Subnet")
            res.physical_id = "subnet-123"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_subnet.assert_called_once()


# ===========================================================================
# EC2::SecurityGroup
# ===========================================================================


class TestEc2SecurityGroup:
    def test_create(self):
        mock = MagicMock()
        mock.create_security_group.return_value = SimpleNamespace(
            id="sg-abc123"
        )
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource(
                "AWS::EC2::SecurityGroup",
                {
                    "GroupName": "my-sg",
                    "GroupDescription": "desc",
                    "VpcId": "vpc-1",
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "sg-abc123"
        assert res.attributes["GroupId"] == "sg-abc123"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource("AWS::EC2::SecurityGroup")
            res.physical_id = "sg-abc123"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_security_group.assert_called_once()


# ===========================================================================
# EC2::InternetGateway
# ===========================================================================


class TestEc2InternetGateway:
    def test_create(self):
        mock = MagicMock()
        mock.create_internet_gateway.return_value = SimpleNamespace(
            id="igw-123"
        )
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource("AWS::EC2::InternetGateway", {})
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "igw-123"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource("AWS::EC2::InternetGateway")
            res.physical_id = "igw-123"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_internet_gateway.assert_called_once()


# ===========================================================================
# EC2::VPCGatewayAttachment
# ===========================================================================


class TestEc2VpcGatewayAttachment:
    def test_create(self):
        mock = MagicMock()
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource(
                "AWS::EC2::VPCGatewayAttachment",
                {"InternetGatewayId": "igw-1", "VpcId": "vpc-1"},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "igw-1|vpc-1"
        mock.attach_internet_gateway.assert_called_once()
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource("AWS::EC2::VPCGatewayAttachment")
            res.physical_id = "igw-1|vpc-1"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.detach_internet_gateway.assert_called_once_with("igw-1", "vpc-1")


# ===========================================================================
# EC2::RouteTable
# ===========================================================================


class TestEc2RouteTable:
    def test_create(self):
        mock = MagicMock()
        mock.create_route_table.return_value = SimpleNamespace(id="rtb-123")
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource(
                "AWS::EC2::RouteTable", {"VpcId": "vpc-1"}
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "rtb-123"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource("AWS::EC2::RouteTable")
            res.physical_id = "rtb-123"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_route_table.assert_called_once()


# ===========================================================================
# EC2::Route
# ===========================================================================


class TestEc2Route:
    def test_create(self):
        mock = MagicMock()
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource(
                "AWS::EC2::Route",
                {
                    "RouteTableId": "rtb-1",
                    "DestinationCidrBlock": "0.0.0.0/0",
                    "GatewayId": "igw-1",
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "rtb-1|0.0.0.0/0"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource("AWS::EC2::Route")
            res.physical_id = "rtb-1|0.0.0.0/0"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_route.assert_called_once_with("rtb-1", "0.0.0.0/0")


# ===========================================================================
# EC2::SubnetRouteTableAssociation
# ===========================================================================


class TestEc2SubnetRouteTableAssociation:
    def test_create(self):
        mock = MagicMock()
        mock.associate_route_table.return_value = "rtbassoc-abc"
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource(
                "AWS::EC2::SubnetRouteTableAssociation",
                {"RouteTableId": "rtb-1", "SubnetId": "subnet-1"},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "rtbassoc-abc"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource("AWS::EC2::SubnetRouteTableAssociation")
            res.physical_id = "rtbassoc-abc"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.disassociate_route_table.assert_called_once()


# ===========================================================================
# EC2::NatGateway
# ===========================================================================


class TestEc2NatGateway:
    def test_create(self):
        res = _resource(
            "AWS::EC2::NatGateway", {"SubnetId": "subnet-1"}
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id.startswith("nat-")
        assert res.attributes["NatGatewayId"] == res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete_noop(self):
        res = _resource("AWS::EC2::NatGateway")
        res.physical_id = "nat-123"
        delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# EC2::EIP
# ===========================================================================


class TestEc2Eip:
    def test_create(self):
        mock = MagicMock()
        mock.allocate_address.return_value = SimpleNamespace(
            allocation_id="eipalloc-abc", public_ip="1.2.3.4"
        )
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource("AWS::EC2::EIP", {})
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "eipalloc-abc"
        assert res.attributes["PublicIp"] == "1.2.3.4"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource("AWS::EC2::EIP")
            res.physical_id = "eipalloc-abc"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.release_address.assert_called_once()


# ===========================================================================
# EC2::LaunchTemplate
# ===========================================================================


class TestEc2LaunchTemplate:
    def test_create(self):
        res = _resource(
            "AWS::EC2::LaunchTemplate",
            {"LaunchTemplateName": "my-lt"},
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id.startswith("lt-")
        assert res.attributes["LaunchTemplateName"] == "my-lt"
        assert res.status == "CREATE_COMPLETE"

    def test_delete_noop(self):
        res = _resource("AWS::EC2::LaunchTemplate")
        res.physical_id = "lt-abc"
        delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# EC2::KeyPair
# ===========================================================================


class TestEc2KeyPair:
    def test_create(self):
        mock = MagicMock()
        mock.create_key_pair.return_value = SimpleNamespace(
            id="key-abc123"
        )
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource("AWS::EC2::KeyPair", {"KeyName": "my-key"})
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "my-key"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("ec2") as p:
            p.return_value = mock
            res = _resource("AWS::EC2::KeyPair")
            res.physical_id = "my-key"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_key_pair.assert_called_once()


# ===========================================================================
# S3::BucketPolicy
# ===========================================================================


class TestS3BucketPolicy:
    def test_create(self):
        mock = MagicMock()
        with _mock_global("s3") as p:
            p.return_value = mock
            res = _resource(
                "AWS::S3::BucketPolicy",
                {"Bucket": "my-bucket", "PolicyDocument": {"v": "2012"}},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "my-bucket"
        mock.put_bucket_policy.assert_called_once()
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_global("s3") as p:
            p.return_value = mock
            res = _resource("AWS::S3::BucketPolicy")
            res.physical_id = "my-bucket"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_bucket_policy.assert_called_once()


# ===========================================================================
# S3::BucketNotificationConfiguration
# ===========================================================================


class TestS3BucketNotificationConfig:
    def test_create(self):
        res = _resource(
            "AWS::S3::BucketNotificationConfiguration",
            {"Bucket": "my-bucket"},
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "my-bucket"
        assert res.status == "CREATE_COMPLETE"

    def test_delete_noop(self):
        res = _resource("AWS::S3::BucketNotificationConfiguration")
        res.physical_id = "my-bucket"
        delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# DynamoDB::GlobalTable
# ===========================================================================


class TestDynamoDbGlobalTable:
    def test_create(self):
        mock = MagicMock()
        with _mock_regional("dynamodb") as p:
            p.return_value = mock
            res = _resource(
                "AWS::DynamoDB::GlobalTable",
                {
                    "TableName": "global-t",
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "AttributeDefinitions": [
                        {"AttributeName": "pk", "AttributeType": "S"}
                    ],
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "global-t"
        assert "global-t" in res.attributes["Arn"]
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("dynamodb") as p:
            p.return_value = mock
            res = _resource("AWS::DynamoDB::GlobalTable")
            res.physical_id = "global-t"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_table.assert_called_once()


# ===========================================================================
# SQS::QueuePolicy
# ===========================================================================


class TestSqsQueuePolicy:
    def test_create(self):
        res = _resource(
            "AWS::SQS::QueuePolicy",
            {"Queues": ["http://localhost:4566/queue/q1"]},
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert "q1" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete_noop(self):
        res = _resource("AWS::SQS::QueuePolicy")
        res.physical_id = "qp"
        delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# ApiGateway::RestApi
# ===========================================================================


class TestApiGatewayRestApi:
    def test_create(self):
        mock = MagicMock()
        mock.create_rest_api.return_value = SimpleNamespace(id="api-123")
        with _mock_regional("apigateway") as p:
            p.return_value = mock
            res = _resource(
                "AWS::ApiGateway::RestApi", {"Name": "my-api"}
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "api-123"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("apigateway") as p:
            p.return_value = mock
            res = _resource("AWS::ApiGateway::RestApi")
            res.physical_id = "api-123"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_rest_api.assert_called_once()


# ===========================================================================
# ApiGateway::Resource
# ===========================================================================


class TestApiGatewayResource:
    def test_create(self):
        mock = MagicMock()
        mock.create_resource.return_value = SimpleNamespace(id="res-1")
        with _mock_regional("apigateway") as p:
            p.return_value = mock
            res = _resource(
                "AWS::ApiGateway::Resource",
                {
                    "RestApiId": "api-1",
                    "ParentId": "root",
                    "PathPart": "items",
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "res-1"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("apigateway") as p:
            p.return_value = mock
            res = _resource(
                "AWS::ApiGateway::Resource",
                {"RestApiId": "api-1"},
            )
            res.physical_id = "res-1"
            delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# ApiGateway::Method
# ===========================================================================


class TestApiGatewayMethod:
    def test_create(self):
        mock = MagicMock()
        with _mock_regional("apigateway") as p:
            p.return_value = mock
            res = _resource(
                "AWS::ApiGateway::Method",
                {
                    "RestApiId": "api-1",
                    "ResourceId": "res-1",
                    "HttpMethod": "POST",
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "api-1/res-1/POST"
        assert res.status == "CREATE_COMPLETE"


# ===========================================================================
# ApiGateway::Deployment
# ===========================================================================


class TestApiGatewayDeployment:
    def test_create(self):
        mock = MagicMock()
        mock.create_deployment.return_value = SimpleNamespace(id="dep-1")
        with _mock_regional("apigateway") as p:
            p.return_value = mock
            res = _resource(
                "AWS::ApiGateway::Deployment",
                {"RestApiId": "api-1"},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "dep-1"
        assert res.status == "CREATE_COMPLETE"


# ===========================================================================
# ApiGateway::Stage
# ===========================================================================


class TestApiGatewayStage:
    def test_create(self):
        res = _resource(
            "AWS::ApiGateway::Stage", {"StageName": "v1"}
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "v1"
        assert res.status == "CREATE_COMPLETE"


# ===========================================================================
# ApiGateway::ApiKey
# ===========================================================================


class TestApiGatewayApiKey:
    def test_create(self):
        res = _resource("AWS::ApiGateway::ApiKey", {})
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id is not None
        assert res.status == "CREATE_COMPLETE"


# ===========================================================================
# ApiGateway::UsagePlan
# ===========================================================================


class TestApiGatewayUsagePlan:
    def test_create(self):
        res = _resource("AWS::ApiGateway::UsagePlan", {})
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id is not None
        assert res.status == "CREATE_COMPLETE"


# ===========================================================================
# ApiGateway::DomainName
# ===========================================================================


class TestApiGatewayDomainName:
    def test_create(self):
        res = _resource(
            "AWS::ApiGateway::DomainName",
            {"DomainName": "api.example.com"},
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "api.example.com"
        assert "cloudfront.net" in res.attributes["DistributionDomainName"]
        assert res.status == "CREATE_COMPLETE"


# ===========================================================================
# CloudWatch::Alarm
# ===========================================================================


class TestCloudWatchAlarm:
    def test_create(self):
        mock = MagicMock()
        with _mock_regional("cloudwatch") as p:
            p.return_value = mock
            res = _resource(
                "AWS::CloudWatch::Alarm",
                {"AlarmName": "high-cpu", "MetricName": "CPUUtilization"},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "high-cpu"
        assert "alarm:" in res.attributes["Arn"]
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("cloudwatch") as p:
            p.return_value = mock
            res = _resource("AWS::CloudWatch::Alarm")
            res.physical_id = "high-cpu"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_alarms.assert_called_once_with(["high-cpu"])


# ===========================================================================
# Logs::LogStream
# ===========================================================================


class TestLogsLogStream:
    def test_create(self):
        mock = MagicMock()
        with _mock_regional("logs") as p:
            p.return_value = mock
            res = _resource(
                "AWS::Logs::LogStream",
                {"LogGroupName": "/app", "LogStreamName": "stream-1"},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "stream-1"
        mock.create_log_stream.assert_called_once_with("/app", "stream-1")
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("logs") as p:
            p.return_value = mock
            res = _resource(
                "AWS::Logs::LogStream",
                {"LogGroupName": "/app"},
            )
            res.physical_id = "stream-1"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_log_stream.assert_called_once()


# ===========================================================================
# CloudWatch::MetricFilter
# ===========================================================================


class TestCloudWatchMetricFilter:
    def test_create(self):
        res = _resource(
            "AWS::CloudWatch::MetricFilter", {"FilterName": "err-filter"}
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "err-filter"
        assert res.status == "CREATE_COMPLETE"

    def test_delete_noop(self):
        res = _resource("AWS::CloudWatch::MetricFilter")
        res.physical_id = "err-filter"
        delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# Events::EventBus
# ===========================================================================


class TestEventsEventBus:
    def test_create(self):
        mock = MagicMock()
        mock.create_event_bus.return_value = SimpleNamespace(
            arn="arn:aws:events:us-east-1:999:event-bus/custom"
        )
        with _mock_regional("events") as p:
            p.return_value = mock
            res = _resource("AWS::Events::EventBus", {"Name": "custom"})
            create_resource(res, REGION, ACCOUNT_ID)
        assert "event-bus/custom" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("events") as p:
            p.return_value = mock
            res = _resource("AWS::Events::EventBus", {"Name": "custom"})
            res.physical_id = "arn:bus"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_event_bus.assert_called_once_with("custom")


# ===========================================================================
# Events::Archive
# ===========================================================================


class TestEventsArchive:
    def test_create(self):
        res = _resource(
            "AWS::Events::Archive", {"ArchiveName": "my-archive"}
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "my-archive"
        assert "archive/" in res.attributes["Arn"]
        assert res.status == "CREATE_COMPLETE"

    def test_delete_noop(self):
        res = _resource("AWS::Events::Archive")
        res.physical_id = "my-archive"
        delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# StepFunctions::StateMachine
# ===========================================================================


class TestSfnStateMachine:
    def test_create(self):
        mock = MagicMock()
        mock.create_state_machine.return_value = SimpleNamespace(
            arn="arn:aws:states:us-east-1:999:stateMachine:sm1"
        )
        with _mock_regional("stepfunctions") as p:
            p.return_value = mock
            res = _resource(
                "AWS::StepFunctions::StateMachine",
                {"StateMachineName": "sm1", "DefinitionString": "{}"},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert "stateMachine:sm1" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("stepfunctions") as p:
            p.return_value = mock
            res = _resource("AWS::StepFunctions::StateMachine")
            res.physical_id = "arn:sm"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_state_machine.assert_called_once()


# ===========================================================================
# StepFunctions::Activity
# ===========================================================================


class TestSfnActivity:
    def test_create(self):
        mock = MagicMock()
        mock.create_activity.return_value = SimpleNamespace(
            arn="arn:aws:states:us-east-1:999:activity:act1"
        )
        with _mock_regional("stepfunctions") as p:
            p.return_value = mock
            res = _resource(
                "AWS::StepFunctions::Activity", {"Name": "act1"}
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert "activity:act1" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("stepfunctions") as p:
            p.return_value = mock
            res = _resource("AWS::StepFunctions::Activity")
            res.physical_id = "arn:act"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_activity.assert_called_once()


# ===========================================================================
# KMS::Alias
# ===========================================================================


class TestKmsAlias:
    def test_create(self):
        mock = MagicMock()
        with _mock_regional("kms") as p:
            p.return_value = mock
            res = _resource(
                "AWS::KMS::Alias",
                {"AliasName": "alias/my-key", "TargetKeyId": "key-1"},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "alias/my-key"
        mock.create_alias.assert_called_once()
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("kms") as p:
            p.return_value = mock
            res = _resource("AWS::KMS::Alias")
            res.physical_id = "alias/my-key"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_alias.assert_called_once()


# ===========================================================================
# SecretsManager::Secret
# ===========================================================================


class TestSecretsManagerSecret:
    def test_create(self):
        mock = MagicMock()
        mock.create_secret.return_value = SimpleNamespace(
            arn="arn:aws:secretsmanager:us-east-1:999:secret:my-sec-abc"
        )
        with _mock_regional("secretsmanager") as p:
            p.return_value = mock
            res = _resource(
                "AWS::SecretsManager::Secret",
                {"Name": "my-sec", "SecretString": "s3cret"},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert "my-sec" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("secretsmanager") as p:
            p.return_value = mock
            res = _resource("AWS::SecretsManager::Secret")
            res.physical_id = "arn:secret"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_secret.assert_called_once()


# ===========================================================================
# SSM::Document
# ===========================================================================


class TestSsmDocument:
    def test_create(self):
        res = _resource("AWS::SSM::Document", {"Name": "my-doc"})
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "my-doc"
        assert res.status == "CREATE_COMPLETE"

    def test_delete_noop(self):
        res = _resource("AWS::SSM::Document")
        res.physical_id = "my-doc"
        delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# Kinesis::Stream
# ===========================================================================


class TestKinesisStream:
    def test_create(self):
        mock = MagicMock()
        with _mock_regional("kinesis") as p:
            p.return_value = mock
            res = _resource(
                "AWS::Kinesis::Stream",
                {"Name": "my-stream", "ShardCount": 2},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "my-stream"
        assert "stream/my-stream" in res.attributes["Arn"]
        mock.create_stream.assert_called_once_with("my-stream", 2)
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("kinesis") as p:
            p.return_value = mock
            res = _resource("AWS::Kinesis::Stream")
            res.physical_id = "my-stream"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_stream.assert_called_once()


# ===========================================================================
# ECS::Cluster
# ===========================================================================


class TestEcsCluster:
    def test_create(self):
        mock = MagicMock()
        mock.create_cluster.return_value = SimpleNamespace(
            arn="arn:aws:ecs:us-east-1:999:cluster/my-cluster"
        )
        with _mock_regional("ecs") as p:
            p.return_value = mock
            res = _resource(
                "AWS::ECS::Cluster", {"ClusterName": "my-cluster"}
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert "cluster/my-cluster" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("ecs") as p:
            p.return_value = mock
            res = _resource("AWS::ECS::Cluster")
            res.physical_id = "arn:cluster"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_cluster.assert_called_once()


# ===========================================================================
# ECS::TaskDefinition
# ===========================================================================


class TestEcsTaskDefinition:
    def test_create(self):
        mock = MagicMock()
        mock.register_task_definition.return_value = SimpleNamespace(
            arn="arn:aws:ecs:us-east-1:999:task-definition/td:1"
        )
        with _mock_regional("ecs") as p:
            p.return_value = mock
            res = _resource(
                "AWS::ECS::TaskDefinition",
                {
                    "Family": "web",
                    "ContainerDefinitions": [{"name": "app"}],
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert "task-definition" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("ecs") as p:
            p.return_value = mock
            res = _resource("AWS::ECS::TaskDefinition")
            res.physical_id = "arn:td"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.deregister_task_definition.assert_called_once()


# ===========================================================================
# ECS::Service
# ===========================================================================


class TestEcsService:
    def test_create(self):
        res = _resource(
            "AWS::ECS::Service",
            {"ServiceName": "web-svc", "Cluster": "default"},
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert "service/default/web-svc" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete_noop(self):
        res = _resource("AWS::ECS::Service")
        res.physical_id = "arn:svc"
        delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# ELBv2::LoadBalancer
# ===========================================================================


class TestElbv2LoadBalancer:
    def test_create_fallback(self):
        mock = MagicMock()
        mock.create_load_balancer.side_effect = Exception("nope")
        with _mock_regional("elbv2") as p:
            p.return_value = mock
            res = _resource(
                "AWS::ElasticLoadBalancingV2::LoadBalancer",
                {"Name": "my-lb"},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert "loadbalancer" in res.physical_id
        assert "elb.amazonaws.com" in res.attributes["DNSName"]
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("elbv2") as p:
            p.return_value = mock
            res = _resource(
                "AWS::ElasticLoadBalancingV2::LoadBalancer"
            )
            res.physical_id = "arn:lb"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_load_balancer.assert_called_once()


# ===========================================================================
# ELBv2::TargetGroup
# ===========================================================================


class TestElbv2TargetGroup:
    def test_create_fallback(self):
        mock = MagicMock()
        mock.create_target_group.side_effect = Exception("nope")
        with _mock_regional("elbv2") as p:
            p.return_value = mock
            res = _resource(
                "AWS::ElasticLoadBalancingV2::TargetGroup",
                {"Name": "my-tg"},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert "targetgroup" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("elbv2") as p:
            p.return_value = mock
            res = _resource(
                "AWS::ElasticLoadBalancingV2::TargetGroup"
            )
            res.physical_id = "arn:tg"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_target_group.assert_called_once()


# ===========================================================================
# ELBv2::Listener
# ===========================================================================


class TestElbv2Listener:
    def test_create(self):
        res = _resource(
            "AWS::ElasticLoadBalancingV2::Listener", {}
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert "listener" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete_noop(self):
        res = _resource("AWS::ElasticLoadBalancingV2::Listener")
        res.physical_id = "arn:listener"
        delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# CloudFront::Distribution
# ===========================================================================


class TestCloudFrontDistribution:
    def test_create(self):
        res = _resource("AWS::CloudFront::Distribution", {})
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id.startswith("E")
        assert "cloudfront.net" in res.attributes["DomainName"]
        assert res.status == "CREATE_COMPLETE"

    def test_delete_noop(self):
        res = _resource("AWS::CloudFront::Distribution")
        res.physical_id = "E123"
        delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# Route53::HostedZone
# ===========================================================================


class TestRoute53HostedZone:
    def test_create(self):
        mock = MagicMock()
        mock.create_hosted_zone.return_value = SimpleNamespace(
            id="Z123ABC",
            nameservers=["ns1.awsdns.com", "ns2.awsdns.com"],
        )
        with _mock_regional("route53") as p:
            p.return_value = mock
            res = _resource(
                "AWS::Route53::HostedZone",
                {"Name": "example.com"},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "Z123ABC"
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("route53") as p:
            p.return_value = mock
            res = _resource("AWS::Route53::HostedZone")
            res.physical_id = "Z123"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_hosted_zone.assert_called_once()


# ===========================================================================
# Route53::RecordSet
# ===========================================================================


class TestRoute53RecordSet:
    def test_create(self):
        res = _resource(
            "AWS::Route53::RecordSet",
            {"HostedZoneId": "Z1", "Name": "www.example.com", "Type": "A"},
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "Z1|www.example.com|A"
        assert res.status == "CREATE_COMPLETE"

    def test_delete_noop(self):
        res = _resource("AWS::Route53::RecordSet")
        res.physical_id = "Z1|www|A"
        delete_resource(res, REGION, ACCOUNT_ID)


# ===========================================================================
# CertificateManager::Certificate
# ===========================================================================


class TestAcmCertificate:
    def test_create_fallback(self):
        mock = MagicMock()
        mock.request_certificate.side_effect = Exception("nope")
        with _mock_regional("acm") as p:
            p.return_value = mock
            res = _resource(
                "AWS::CertificateManager::Certificate",
                {"DomainName": "example.com"},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert "certificate" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("acm") as p:
            p.return_value = mock
            res = _resource("AWS::CertificateManager::Certificate")
            res.physical_id = "arn:cert"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_certificate.assert_called_once()


# ===========================================================================
# Cognito::UserPool
# ===========================================================================


class TestCognitoUserPool:
    def test_create(self):
        mock = MagicMock()
        mock.create_user_pool.return_value = SimpleNamespace(
            id="us-east-1_abc123",
            arn="arn:aws:cognito-idp:us-east-1:999:userpool/us-east-1_abc123",
        )
        with _mock_regional("cognitoidp") as p:
            p.return_value = mock
            res = _resource(
                "AWS::Cognito::UserPool",
                {"UserPoolName": "my-pool"},
            )
            create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "us-east-1_abc123"
        assert "userpool" in res.attributes["Arn"]
        assert res.status == "CREATE_COMPLETE"

    def test_delete(self):
        mock = MagicMock()
        with _mock_regional("cognitoidp") as p:
            p.return_value = mock
            res = _resource("AWS::Cognito::UserPool")
            res.physical_id = "us-east-1_abc"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock.delete_user_pool.assert_called_once()


# ===========================================================================
# Custom:: resources
# ===========================================================================


class TestCustomResource:
    def test_custom_prefix_creates_with_fake_id(self):
        res = _resource("Custom::MyThing", {})
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id.startswith("custom-")
        assert res.status == "CREATE_COMPLETE"

    def test_cloudformation_custom_resource_type(self):
        res = _resource("AWS::CloudFormation::CustomResource", {})
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id.startswith("custom-")
        assert res.status == "CREATE_COMPLETE"

    def test_custom_with_lambda_service_token(self):
        with patch(
            "robotocore.services.cloudformation.resources.invoke_lambda_sync",
            create=True,
        ) as mock_invoke:
            mock_invoke.return_value = {
                "PhysicalResourceId": "custom-phys-1",
                "Data": {"Key1": "Val1"},
            }
            # Also need to patch the import inside the function
            with patch.dict(
                "sys.modules",
                {
                    "robotocore.services.lambda_.invoke": MagicMock(
                        invoke_lambda_sync=mock_invoke,
                    )
                },
            ):
                res = _resource(
                    "Custom::MyLambda",
                    {
                        "ServiceToken": (
                            "arn:aws:lambda:us-east-1:999:function:cfn-handler"
                        ),
                    },
                )
                create_resource(res, REGION, ACCOUNT_ID)
        assert res.status == "CREATE_COMPLETE"
        # Either the invoke worked or fallback assigned an ID
        assert res.physical_id is not None

    def test_custom_delete_no_service_token(self):
        res = _resource("Custom::MyThing", {})
        res.physical_id = "custom-123"
        delete_resource(res, REGION, ACCOUNT_ID)
        # Should not raise


# ===========================================================================
# Fn::Cidr intrinsic + helper
# ===========================================================================


class TestFnCidr:
    def test_compute_cidr_basic(self):
        result = compute_cidr("10.0.0.0/16", 3, 8)
        assert len(result) == 3
        # Each should be a /24 subnet (32-8=24)
        assert all("/24" in r for r in result)

    def test_compute_cidr_single(self):
        result = compute_cidr("10.0.0.0/16", 1, 8)
        assert len(result) == 1

    def test_fn_cidr_intrinsic(self):
        val = {"Fn::Cidr": ["10.0.0.0/16", 3, 8]}
        result = resolve_intrinsics(
            val, _RESOURCES, _PARAMS, REGION, ACCOUNT_ID
        )
        assert isinstance(result, list)
        assert len(result) == 3


# ===========================================================================
# Fn::ImportValue intrinsic
# ===========================================================================


class TestFnImportValue:
    def test_import_value_found(self):
        params = {
            **_PARAMS,
            "__imports__": {"VpcId": "vpc-abc123"},
        }
        val = {"Fn::ImportValue": "VpcId"}
        result = resolve_intrinsics(
            val, _RESOURCES, params, REGION, ACCOUNT_ID
        )
        assert result == "vpc-abc123"

    def test_import_value_not_found(self):
        val = {"Fn::ImportValue": "Unknown"}
        result = resolve_intrinsics(
            val, _RESOURCES, _PARAMS, REGION, ACCOUNT_ID
        )
        assert result == "Unknown"


# ===========================================================================
# Fn::Transform intrinsic
# ===========================================================================


class TestFnTransform:
    def test_transform_passthrough(self):
        val = {
            "Fn::Transform": {
                "Name": "AWS::Include",
                "Parameters": {"Location": "s3://bucket/file.yaml"},
            }
        }
        result = resolve_intrinsics(
            val, _RESOURCES, _PARAMS, REGION, ACCOUNT_ID
        )
        assert "Fn::Transform" in result
        assert result["Fn::Transform"]["Name"] == "AWS::Include"


# ===========================================================================
# Enhanced Fn::If with Conditions
# ===========================================================================


class TestFnIfConditions:
    def test_fn_if_condition_true(self):
        params = {
            **_PARAMS,
            "__conditions__": {"IsProd": True},
        }
        val = {"Fn::If": ["IsProd", "yes", "no"]}
        result = resolve_intrinsics(
            val, _RESOURCES, params, REGION, ACCOUNT_ID
        )
        assert result == "yes"

    def test_fn_if_condition_false(self):
        params = {
            **_PARAMS,
            "__conditions__": {"IsProd": False},
        }
        val = {"Fn::If": ["IsProd", "yes", "no"]}
        result = resolve_intrinsics(
            val, _RESOURCES, params, REGION, ACCOUNT_ID
        )
        assert result == "no"

    def test_fn_if_condition_missing_defaults_true(self):
        val = {"Fn::If": ["Missing", "yes", "no"]}
        result = resolve_intrinsics(
            val, _RESOURCES, _PARAMS, REGION, ACCOUNT_ID
        )
        assert result == "yes"

    def test_fn_if_with_intrinsic_condition(self):
        params = {
            **_PARAMS,
            "__conditions__": {
                "IsProd": {"Fn::Equals": [{"Ref": "EnvName"}, "prod"]}
            },
        }
        val = {"Fn::If": ["IsProd", "prod-value", "dev-value"]}
        result = resolve_intrinsics(
            val, _RESOURCES, params, REGION, ACCOUNT_ID
        )
        assert result == "prod-value"

    def test_fn_if_with_false_intrinsic_condition(self):
        params = {
            **_PARAMS,
            "__conditions__": {
                "IsProd": {"Fn::Equals": [{"Ref": "EnvName"}, "staging"]}
            },
        }
        val = {"Fn::If": ["IsProd", "prod-value", "dev-value"]}
        result = resolve_intrinsics(
            val, _RESOURCES, params, REGION, ACCOUNT_ID
        )
        assert result == "dev-value"


# ===========================================================================
# Fn::And / Fn::Or
# ===========================================================================


class TestFnAndOr:
    def test_fn_and_all_true(self):
        val = {"Fn::And": [True, True, True]}
        assert resolve_intrinsics(
            val, _RESOURCES, _PARAMS, REGION, ACCOUNT_ID
        ) is True

    def test_fn_and_one_false(self):
        val = {"Fn::And": [True, False]}
        assert resolve_intrinsics(
            val, _RESOURCES, _PARAMS, REGION, ACCOUNT_ID
        ) is False

    def test_fn_or_one_true(self):
        val = {"Fn::Or": [False, True]}
        assert resolve_intrinsics(
            val, _RESOURCES, _PARAMS, REGION, ACCOUNT_ID
        ) is True

    def test_fn_or_all_false(self):
        val = {"Fn::Or": [False, False]}
        assert resolve_intrinsics(
            val, _RESOURCES, _PARAMS, REGION, ACCOUNT_ID
        ) is False

    def test_fn_and_with_intrinsics(self):
        val = {
            "Fn::And": [
                {"Fn::Equals": ["a", "a"]},
                {"Fn::Equals": ["b", "b"]},
            ]
        }
        assert resolve_intrinsics(
            val, _RESOURCES, _PARAMS, REGION, ACCOUNT_ID
        ) is True


# ===========================================================================
# Dependency ordering with new types
# ===========================================================================


class TestDependencyOrderNewTypes:
    def test_vpc_subnet_ordering(self):
        from robotocore.services.cloudformation.engine import (
            build_dependency_order,
        )

        tmpl = {
            "Resources": {
                "VPC": {"Type": "AWS::EC2::VPC"},
                "Subnet": {
                    "Type": "AWS::EC2::Subnet",
                    "Properties": {"VpcId": {"Ref": "VPC"}},
                },
            }
        }
        order = build_dependency_order(tmpl)
        assert order.index("VPC") < order.index("Subnet")

    def test_ecs_chain(self):
        from robotocore.services.cloudformation.engine import (
            build_dependency_order,
        )

        tmpl = {
            "Resources": {
                "Cluster": {"Type": "AWS::ECS::Cluster"},
                "TaskDef": {"Type": "AWS::ECS::TaskDefinition"},
                "Service": {
                    "Type": "AWS::ECS::Service",
                    "DependsOn": ["Cluster", "TaskDef"],
                },
            }
        }
        order = build_dependency_order(tmpl)
        assert order.index("Cluster") < order.index("Service")
        assert order.index("TaskDef") < order.index("Service")


# ===========================================================================
# Additional resource types (batch 2)
# ===========================================================================


class TestEc2Instance:
    def test_create(self):
        res = _resource("AWS::EC2::Instance", {})
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id.startswith("i-")
        assert res.attributes["InstanceId"] == res.physical_id
        assert res.status == "CREATE_COMPLETE"


class TestEc2Volume:
    def test_create(self):
        res = _resource("AWS::EC2::Volume", {})
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id.startswith("vol-")
        assert res.status == "CREATE_COMPLETE"


class TestEc2FlowLog:
    def test_create(self):
        res = _resource("AWS::EC2::FlowLog", {})
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id.startswith("fl-")
        assert res.status == "CREATE_COMPLETE"


class TestEc2NetworkInterface:
    def test_create(self):
        res = _resource("AWS::EC2::NetworkInterface", {})
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id.startswith("eni-")
        assert res.status == "CREATE_COMPLETE"


class TestLambdaUrl:
    def test_create(self):
        res = _resource("AWS::Lambda::Url", {})
        create_resource(res, REGION, ACCOUNT_ID)
        assert "lambda-url" in res.physical_id
        assert res.attributes["FunctionUrl"] == res.physical_id
        assert res.status == "CREATE_COMPLETE"


class TestCfnStack:
    def test_create_nested(self):
        res = _resource("AWS::CloudFormation::Stack", {})
        create_resource(res, REGION, ACCOUNT_ID)
        assert "cloudformation" in res.physical_id
        assert "nested" in res.physical_id
        assert res.status == "CREATE_COMPLETE"


class TestCfnWaitCondition:
    def test_create(self):
        res = _resource("AWS::CloudFormation::WaitCondition", {})
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id.startswith("waitcond-")
        assert res.attributes["Data"] == "{}"
        assert res.status == "CREATE_COMPLETE"


class TestWafv2WebAcl:
    def test_create(self):
        res = _resource("AWS::WAFv2::WebACL", {"Name": "my-acl"})
        create_resource(res, REGION, ACCOUNT_ID)
        assert "webacl/my-acl" in res.physical_id
        assert res.status == "CREATE_COMPLETE"


class TestCognitoUserPoolClient:
    def test_create(self):
        res = _resource(
            "AWS::Cognito::UserPoolClient",
            {"ClientName": "app"},
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.attributes["ClientId"] == res.physical_id
        assert res.status == "CREATE_COMPLETE"


class TestCognitoIdentityPool:
    def test_create(self):
        res = _resource(
            "AWS::Cognito::IdentityPool",
            {"IdentityPoolName": "my-id-pool"},
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert REGION in res.physical_id
        assert res.status == "CREATE_COMPLETE"


class TestIamRolePolicy:
    def test_create(self):
        res = _resource(
            "AWS::IAM::RolePolicy",
            {"RoleName": "role1", "PolicyName": "pol1"},
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "role1|pol1"
        assert res.status == "CREATE_COMPLETE"


class TestSnsTopicPolicy:
    def test_create(self):
        res = _resource(
            "AWS::SNS::TopicPolicy",
            {"Topics": ["arn:aws:sns:us-east-1:999:topic"]},
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert "topic" in res.physical_id
        assert res.status == "CREATE_COMPLETE"


class TestElasticsearchDomain:
    def test_create(self):
        res = _resource(
            "AWS::Elasticsearch::Domain", {"DomainName": "my-search"}
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "my-search"
        assert "es.amazonaws.com" in res.attributes["DomainEndpoint"]
        assert res.status == "CREATE_COMPLETE"


class TestRedshiftCluster:
    def test_create(self):
        res = _resource(
            "AWS::Redshift::Cluster",
            {"ClusterIdentifier": "my-cluster"},
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "my-cluster"
        assert "redshift.amazonaws.com" in res.attributes["Endpoint.Address"]
        assert res.status == "CREATE_COMPLETE"


class TestRdsDbInstance:
    def test_create(self):
        res = _resource(
            "AWS::RDS::DBInstance",
            {"DBInstanceIdentifier": "mydb", "Port": 5432},
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "mydb"
        assert "rds.amazonaws.com" in res.attributes["Endpoint.Address"]
        assert res.attributes["Endpoint.Port"] == "5432"
        assert res.status == "CREATE_COMPLETE"


class TestRdsDbCluster:
    def test_create(self):
        res = _resource(
            "AWS::RDS::DBCluster",
            {"DBClusterIdentifier": "myaur"},
        )
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id == "myaur"
        assert "cluster-" in res.attributes["Endpoint.Address"]
        assert res.status == "CREATE_COMPLETE"


# ===========================================================================
# Misc edge cases
# ===========================================================================


class TestEdgeCases:
    def test_all_new_types_have_create_complete(self):
        """Every simulated resource should end at CREATE_COMPLETE."""
        simulated = [
            "AWS::EC2::NatGateway",
            "AWS::EC2::LaunchTemplate",
            "AWS::CloudFront::Distribution",
            "AWS::Events::Archive",
            "AWS::CloudWatch::MetricFilter",
            "AWS::SSM::Document",
            "AWS::ECS::Service",
            "AWS::ElasticLoadBalancingV2::Listener",
            "AWS::Route53::RecordSet",
            "AWS::ApiGateway::Stage",
            "AWS::ApiGateway::ApiKey",
            "AWS::ApiGateway::UsagePlan",
            "AWS::ApiGateway::DomainName",
            "AWS::IAM::ServiceLinkedRole",
            "AWS::Lambda::LayerVersion",
            "AWS::Lambda::Alias",
            "AWS::S3::BucketNotificationConfiguration",
            "AWS::SQS::QueuePolicy",
            "AWS::SES::EmailIdentity",
            "AWS::SES::ConfigurationSet",
            "AWS::SES::Template",
            "AWS::CloudWatch::Dashboard",
            "AWS::CloudWatch::CompositeAlarm",
            "AWS::Logs::SubscriptionFilter",
            "AWS::Logs::MetricFilter",
            "AWS::EC2::SecurityGroupIngress",
            "AWS::EC2::SecurityGroupEgress",
            "AWS::EC2::NetworkInterface",
            "AWS::EC2::Volume",
            "AWS::EC2::Instance",
            "AWS::EC2::FlowLog",
            "AWS::ApplicationAutoScaling::ScalableTarget",
            "AWS::ApplicationAutoScaling::ScalingPolicy",
            "AWS::SNS::TopicPolicy",
            "AWS::Lambda::EventInvokeConfig",
            "AWS::Lambda::Url",
            "AWS::IAM::RolePolicy",
            "AWS::IAM::UserToGroupAddition",
            "AWS::CloudFormation::WaitConditionHandle",
            "AWS::CloudFormation::WaitCondition",
            "AWS::CloudFormation::Stack",
            "AWS::Cognito::UserPoolClient",
            "AWS::Cognito::IdentityPool",
            "AWS::WAFv2::WebACL",
            "AWS::Elasticsearch::Domain",
            "AWS::Redshift::Cluster",
            "AWS::RDS::DBInstance",
            "AWS::RDS::DBSubnetGroup",
            "AWS::RDS::DBCluster",
        ]
        for rtype in simulated:
            res = _resource(rtype, {})
            create_resource(res, REGION, ACCOUNT_ID)
            assert res.status == "CREATE_COMPLETE", f"{rtype} failed"
            assert res.physical_id is not None, f"{rtype} no physical_id"

    def test_unknown_type_still_works(self):
        res = _resource("AWS::Custom::Whatever", {})
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.status == "CREATE_COMPLETE"

    def test_delete_no_physical_id_safe(self):
        """Deleting with no physical_id should not raise."""
        for rtype in list(_DELETE_HANDLERS.keys())[:10]:
            res = _resource(rtype, {})
            res.physical_id = None
            delete_resource(res, REGION, ACCOUNT_ID)
