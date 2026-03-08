"""ELBv2 (Application/Network Load Balancer) compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def elbv2():
    return make_client("elbv2")


@pytest.fixture
def ec2():
    return make_client("ec2")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def vpc_with_subnets(ec2):
    """Create a VPC with two subnets in different AZs for load balancer tests."""
    cidr = "10.60.0.0/16"
    vpc = ec2.create_vpc(CidrBlock=cidr)
    vpc_id = vpc["Vpc"]["VpcId"]
    sub1 = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.60.1.0/24", AvailabilityZone="us-east-1a")
    sub2 = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.60.2.0/24", AvailabilityZone="us-east-1b")
    s1 = sub1["Subnet"]["SubnetId"]
    s2 = sub2["Subnet"]["SubnetId"]

    yield {"vpc_id": vpc_id, "subnet_ids": [s1, s2]}

    # Cleanup
    ec2.delete_subnet(SubnetId=s1)
    ec2.delete_subnet(SubnetId=s2)
    ec2.delete_vpc(VpcId=vpc_id)


class TestELBv2LoadBalancerOperations:
    def test_create_and_describe_alb(self, elbv2, vpc_with_subnets):
        name = _unique("alb")
        resp = elbv2.create_load_balancer(
            Name=name,
            Subnets=vpc_with_subnets["subnet_ids"],
            Type="application",
        )
        lb = resp["LoadBalancers"][0]
        lb_arn = lb["LoadBalancerArn"]
        try:
            assert lb["LoadBalancerName"] == name
            assert lb["Type"] == "application"
            assert "LoadBalancerArn" in lb
            assert lb["State"]["Code"] in ("provisioning", "active")

            # Describe by ARN
            desc = elbv2.describe_load_balancers(LoadBalancerArns=[lb_arn])
            assert len(desc["LoadBalancers"]) == 1
            assert desc["LoadBalancers"][0]["LoadBalancerName"] == name

            # Describe by Name
            desc2 = elbv2.describe_load_balancers(Names=[name])
            assert len(desc2["LoadBalancers"]) == 1
        finally:
            elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)

    def test_create_nlb(self, elbv2, vpc_with_subnets):
        name = _unique("nlb")
        resp = elbv2.create_load_balancer(
            Name=name,
            Subnets=vpc_with_subnets["subnet_ids"],
            Type="network",
        )
        lb = resp["LoadBalancers"][0]
        try:
            assert lb["Type"] == "network"
            assert lb["LoadBalancerName"] == name
        finally:
            elbv2.delete_load_balancer(LoadBalancerArn=lb["LoadBalancerArn"])

    def test_list_load_balancers(self, elbv2, vpc_with_subnets):
        name = _unique("alb")
        resp = elbv2.create_load_balancer(
            Name=name,
            Subnets=vpc_with_subnets["subnet_ids"],
            Type="application",
        )
        lb_arn = resp["LoadBalancers"][0]["LoadBalancerArn"]
        try:
            all_lbs = elbv2.describe_load_balancers()
            names = [lb["LoadBalancerName"] for lb in all_lbs["LoadBalancers"]]
            assert name in names
        finally:
            elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)

    def test_load_balancer_tags(self, elbv2, vpc_with_subnets):
        name = _unique("alb")
        resp = elbv2.create_load_balancer(
            Name=name,
            Subnets=vpc_with_subnets["subnet_ids"],
            Type="application",
        )
        lb_arn = resp["LoadBalancers"][0]["LoadBalancerArn"]
        try:
            # Add tags
            elbv2.add_tags(
                ResourceArns=[lb_arn],
                Tags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "platform"},
                ],
            )
            tags_resp = elbv2.describe_tags(ResourceArns=[lb_arn])
            tags = {t["Key"]: t["Value"] for t in tags_resp["TagDescriptions"][0]["Tags"]}
            assert tags["env"] == "test"
            assert tags["team"] == "platform"

            # Remove one tag
            elbv2.remove_tags(ResourceArns=[lb_arn], TagKeys=["env"])
            tags_resp2 = elbv2.describe_tags(ResourceArns=[lb_arn])
            tag_keys = [t["Key"] for t in tags_resp2["TagDescriptions"][0]["Tags"]]
            assert "env" not in tag_keys
            assert "team" in tag_keys
        finally:
            elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)

    def test_load_balancer_attributes(self, elbv2, vpc_with_subnets):
        name = _unique("alb")
        resp = elbv2.create_load_balancer(
            Name=name,
            Subnets=vpc_with_subnets["subnet_ids"],
            Type="application",
        )
        lb_arn = resp["LoadBalancers"][0]["LoadBalancerArn"]
        try:
            # Describe default attributes
            attrs = elbv2.describe_load_balancer_attributes(LoadBalancerArn=lb_arn)
            assert len(attrs["Attributes"]) > 0

            # Modify attributes
            mod = elbv2.modify_load_balancer_attributes(
                LoadBalancerArn=lb_arn,
                Attributes=[{"Key": "idle_timeout.timeout_seconds", "Value": "120"}],
            )
            assert len(mod["Attributes"]) > 0
            attr_dict = {a["Key"]: a["Value"] for a in mod["Attributes"]}
            assert attr_dict["idle_timeout.timeout_seconds"] == "120"
        finally:
            elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)

    def test_delete_load_balancer(self, elbv2, vpc_with_subnets):
        name = _unique("alb")
        resp = elbv2.create_load_balancer(
            Name=name,
            Subnets=vpc_with_subnets["subnet_ids"],
            Type="application",
        )
        lb_arn = resp["LoadBalancers"][0]["LoadBalancerArn"]
        elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)

        # After deletion, describe by name should fail
        with pytest.raises(elbv2.exceptions.LoadBalancerNotFoundException):
            elbv2.describe_load_balancers(Names=[name])


class TestELBv2TargetGroupOperations:
    def test_create_and_describe_target_group(self, elbv2, vpc_with_subnets):
        name = _unique("tg")
        resp = elbv2.create_target_group(
            Name=name,
            Protocol="HTTP",
            Port=80,
            VpcId=vpc_with_subnets["vpc_id"],
        )
        tg = resp["TargetGroups"][0]
        tg_arn = tg["TargetGroupArn"]
        try:
            assert tg["TargetGroupName"] == name
            assert tg["Protocol"] == "HTTP"
            assert tg["Port"] == 80

            # Describe by ARN
            desc = elbv2.describe_target_groups(TargetGroupArns=[tg_arn])
            assert len(desc["TargetGroups"]) == 1
            assert desc["TargetGroups"][0]["TargetGroupName"] == name

            # Describe by name
            desc2 = elbv2.describe_target_groups(Names=[name])
            assert len(desc2["TargetGroups"]) == 1
        finally:
            elbv2.delete_target_group(TargetGroupArn=tg_arn)

    def test_modify_target_group(self, elbv2, vpc_with_subnets):
        name = _unique("tg")
        resp = elbv2.create_target_group(
            Name=name,
            Protocol="HTTP",
            Port=80,
            VpcId=vpc_with_subnets["vpc_id"],
        )
        tg_arn = resp["TargetGroups"][0]["TargetGroupArn"]
        try:
            mod = elbv2.modify_target_group(
                TargetGroupArn=tg_arn,
                HealthCheckPath="/health",
                HealthCheckIntervalSeconds=15,
            )
            tg = mod["TargetGroups"][0]
            assert tg["HealthCheckPath"] == "/health"
            assert tg["HealthCheckIntervalSeconds"] == 15
        finally:
            elbv2.delete_target_group(TargetGroupArn=tg_arn)

    def test_target_group_attributes(self, elbv2, vpc_with_subnets):
        name = _unique("tg")
        resp = elbv2.create_target_group(
            Name=name,
            Protocol="HTTP",
            Port=80,
            VpcId=vpc_with_subnets["vpc_id"],
        )
        tg_arn = resp["TargetGroups"][0]["TargetGroupArn"]
        try:
            attrs = elbv2.describe_target_group_attributes(TargetGroupArn=tg_arn)
            assert len(attrs["Attributes"]) > 0
        finally:
            elbv2.delete_target_group(TargetGroupArn=tg_arn)

    def test_register_and_deregister_targets(self, elbv2, vpc_with_subnets):
        name = _unique("tg")
        resp = elbv2.create_target_group(
            Name=name,
            Protocol="HTTP",
            Port=80,
            VpcId=vpc_with_subnets["vpc_id"],
        )
        tg_arn = resp["TargetGroups"][0]["TargetGroupArn"]
        try:
            # Register a target
            elbv2.register_targets(
                TargetGroupArn=tg_arn,
                Targets=[{"Id": "i-1234567890abcdef0", "Port": 80}],
            )

            # Describe target health
            health = elbv2.describe_target_health(TargetGroupArn=tg_arn)
            assert len(health["TargetHealthDescriptions"]) == 1
            assert health["TargetHealthDescriptions"][0]["Target"]["Id"] == "i-1234567890abcdef0"

            # Deregister
            elbv2.deregister_targets(
                TargetGroupArn=tg_arn,
                Targets=[{"Id": "i-1234567890abcdef0", "Port": 80}],
            )
            health2 = elbv2.describe_target_health(TargetGroupArn=tg_arn)
            assert len(health2["TargetHealthDescriptions"]) == 0
        finally:
            elbv2.delete_target_group(TargetGroupArn=tg_arn)

    def test_delete_target_group(self, elbv2, vpc_with_subnets):
        name = _unique("tg")
        resp = elbv2.create_target_group(
            Name=name,
            Protocol="HTTP",
            Port=80,
            VpcId=vpc_with_subnets["vpc_id"],
        )
        tg_arn = resp["TargetGroups"][0]["TargetGroupArn"]
        elbv2.delete_target_group(TargetGroupArn=tg_arn)

        # After deletion, listing should not include it
        desc = elbv2.describe_target_groups()
        arns = [tg["TargetGroupArn"] for tg in desc["TargetGroups"]]
        assert tg_arn not in arns


class TestELBv2ListenerOperations:
    def test_create_and_describe_listener(self, elbv2, vpc_with_subnets):
        lb_name = _unique("alb")
        tg_name = _unique("tg")

        lb = elbv2.create_load_balancer(
            Name=lb_name,
            Subnets=vpc_with_subnets["subnet_ids"],
            Type="application",
        )
        lb_arn = lb["LoadBalancers"][0]["LoadBalancerArn"]

        tg = elbv2.create_target_group(
            Name=tg_name,
            Protocol="HTTP",
            Port=80,
            VpcId=vpc_with_subnets["vpc_id"],
        )
        tg_arn = tg["TargetGroups"][0]["TargetGroupArn"]

        try:
            listener = elbv2.create_listener(
                LoadBalancerArn=lb_arn,
                Protocol="HTTP",
                Port=80,
                DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
            )
            listener_arn = listener["Listeners"][0]["ListenerArn"]
            assert listener["Listeners"][0]["Port"] == 80
            assert listener["Listeners"][0]["Protocol"] == "HTTP"

            # Describe by LB ARN
            desc = elbv2.describe_listeners(LoadBalancerArn=lb_arn)
            assert len(desc["Listeners"]) == 1
            assert desc["Listeners"][0]["ListenerArn"] == listener_arn

            # Describe by listener ARN
            desc2 = elbv2.describe_listeners(ListenerArns=[listener_arn])
            assert len(desc2["Listeners"]) == 1

            # Delete listener
            elbv2.delete_listener(ListenerArn=listener_arn)
            desc3 = elbv2.describe_listeners(LoadBalancerArn=lb_arn)
            assert len(desc3["Listeners"]) == 0
        finally:
            elbv2.delete_target_group(TargetGroupArn=tg_arn)
            elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)

    def test_multiple_listeners(self, elbv2, vpc_with_subnets):
        lb_name = _unique("alb")
        tg_name = _unique("tg")

        lb = elbv2.create_load_balancer(
            Name=lb_name,
            Subnets=vpc_with_subnets["subnet_ids"],
            Type="application",
        )
        lb_arn = lb["LoadBalancers"][0]["LoadBalancerArn"]

        tg = elbv2.create_target_group(
            Name=tg_name,
            Protocol="HTTP",
            Port=80,
            VpcId=vpc_with_subnets["vpc_id"],
        )
        tg_arn = tg["TargetGroups"][0]["TargetGroupArn"]

        listener_arns = []
        try:
            for port in [80, 8080]:
                resp = elbv2.create_listener(
                    LoadBalancerArn=lb_arn,
                    Protocol="HTTP",
                    Port=port,
                    DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
                )
                listener_arns.append(resp["Listeners"][0]["ListenerArn"])

            desc = elbv2.describe_listeners(LoadBalancerArn=lb_arn)
            assert len(desc["Listeners"]) == 2
            ports = sorted([listener["Port"] for listener in desc["Listeners"]])
            assert ports == [80, 8080]
        finally:
            for arn in listener_arns:
                elbv2.delete_listener(ListenerArn=arn)
            elbv2.delete_target_group(TargetGroupArn=tg_arn)
            elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)


class TestELBv2MetadataOperations:
    def test_describe_account_limits(self, elbv2):
        resp = elbv2.describe_account_limits()
        assert "Limits" in resp
        assert len(resp["Limits"]) > 0
        # Should contain well-known limit names
        limit_names = [lim["Name"] for lim in resp["Limits"]]
        assert any(
            "load-balancers" in n.lower() or "target-groups" in n.lower() for n in limit_names
        )

    def test_describe_ssl_policies(self, elbv2):
        resp = elbv2.describe_ssl_policies()
        assert "SslPolicies" in resp
        assert len(resp["SslPolicies"]) > 0
        # Each policy should have a name and ciphers
        policy = resp["SslPolicies"][0]
        assert "Name" in policy
        assert "Ciphers" in policy


class TestELBv2ListenerCertificateOperations:
    """Tests for AddListenerCertificates, DescribeListenerCertificates, RemoveListenerCertificates."""

    @pytest.fixture
    def acm(self):
        return make_client("acm")

    @pytest.fixture
    def https_listener(self, elbv2, acm, vpc_with_subnets):
        """Create an HTTPS listener with a default certificate for certificate tests."""
        # Import a self-signed cert into ACM
        import datetime

        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID

        now = datetime.datetime.now(datetime.UTC)

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test.example.com")])
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + datetime.timedelta(days=365))
            .sign(key, hashes.SHA256())
        )
        cert_pem = cert.public_bytes(serialization.Encoding.PEM)
        key_pem = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )

        acm_resp = acm.import_certificate(Certificate=cert_pem, PrivateKey=key_pem)
        cert_arn = acm_resp["CertificateArn"]

        # Create a second cert for add/remove tests
        key2 = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject2 = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test2.example.com")])
        cert2 = (
            x509.CertificateBuilder()
            .subject_name(subject2)
            .issuer_name(subject2)
            .public_key(key2.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + datetime.timedelta(days=365))
            .sign(key2, hashes.SHA256())
        )
        cert2_pem = cert2.public_bytes(serialization.Encoding.PEM)
        key2_pem = key2.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
        acm_resp2 = acm.import_certificate(Certificate=cert2_pem, PrivateKey=key2_pem)
        cert_arn2 = acm_resp2["CertificateArn"]

        lb_name = _unique("alb")
        tg_name = _unique("tg")

        lb = elbv2.create_load_balancer(
            Name=lb_name,
            Subnets=vpc_with_subnets["subnet_ids"],
            Type="application",
        )
        lb_arn = lb["LoadBalancers"][0]["LoadBalancerArn"]

        tg = elbv2.create_target_group(
            Name=tg_name,
            Protocol="HTTP",
            Port=80,
            VpcId=vpc_with_subnets["vpc_id"],
        )
        tg_arn = tg["TargetGroups"][0]["TargetGroupArn"]

        listener = elbv2.create_listener(
            LoadBalancerArn=lb_arn,
            Protocol="HTTPS",
            Port=443,
            Certificates=[{"CertificateArn": cert_arn}],
            DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
        )
        listener_arn = listener["Listeners"][0]["ListenerArn"]

        yield {
            "listener_arn": listener_arn,
            "default_cert_arn": cert_arn,
            "extra_cert_arn": cert_arn2,
            "lb_arn": lb_arn,
            "tg_arn": tg_arn,
        }

        # Cleanup
        elbv2.delete_listener(ListenerArn=listener_arn)
        elbv2.delete_target_group(TargetGroupArn=tg_arn)
        elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)
        acm.delete_certificate(CertificateArn=cert_arn)
        acm.delete_certificate(CertificateArn=cert_arn2)

    def test_describe_listener_certificates(self, elbv2, https_listener):
        """DescribeListenerCertificates returns the default certificate."""
        resp = elbv2.describe_listener_certificates(
            ListenerArn=https_listener["listener_arn"],
        )
        assert "Certificates" in resp
        assert len(resp["Certificates"]) >= 1
        cert_arns = [c["CertificateArn"] for c in resp["Certificates"]]
        assert https_listener["default_cert_arn"] in cert_arns

    def test_add_listener_certificates(self, elbv2, https_listener):
        """AddListenerCertificates adds an extra certificate to the listener."""
        resp = elbv2.add_listener_certificates(
            ListenerArn=https_listener["listener_arn"],
            Certificates=[{"CertificateArn": https_listener["extra_cert_arn"]}],
        )
        assert "Certificates" in resp
        added_arns = [c["CertificateArn"] for c in resp["Certificates"]]
        assert https_listener["extra_cert_arn"] in added_arns

    def test_remove_listener_certificates(self, elbv2, https_listener):
        """RemoveListenerCertificates removes a non-default certificate."""
        # First add the extra cert
        elbv2.add_listener_certificates(
            ListenerArn=https_listener["listener_arn"],
            Certificates=[{"CertificateArn": https_listener["extra_cert_arn"]}],
        )

        # Now remove it
        elbv2.remove_listener_certificates(
            ListenerArn=https_listener["listener_arn"],
            Certificates=[{"CertificateArn": https_listener["extra_cert_arn"]}],
        )

        # Verify it's gone
        resp = elbv2.describe_listener_certificates(
            ListenerArn=https_listener["listener_arn"],
        )
        cert_arns = [c["CertificateArn"] for c in resp["Certificates"]]
        assert https_listener["extra_cert_arn"] not in cert_arns


class TestElbv2AutoCoverage:
    """Auto-generated coverage tests for elbv2."""

    @pytest.fixture
    def client(self):
        return make_client("elbv2")

    def test_describe_ssl_policies(self, client):
        """DescribeSSLPolicies returns a response."""
        resp = client.describe_ssl_policies()
        assert "SslPolicies" in resp
