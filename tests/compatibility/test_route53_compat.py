"""Route53 compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def route53():
    return make_client("route53")


@pytest.fixture
def hosted_zone(route53):
    response = route53.create_hosted_zone(
        Name="example.com",
        CallerReference="unique-ref-123",
    )
    zone_id = response["HostedZone"]["Id"].split("/")[-1]
    yield zone_id
    route53.delete_hosted_zone(Id=zone_id)


class TestRoute53Operations:
    def test_create_hosted_zone(self, route53):
        response = route53.create_hosted_zone(
            Name="test.example.com",
            CallerReference="ref-create-test",
        )
        assert "HostedZone" in response
        zone_id = response["HostedZone"]["Id"].split("/")[-1]
        route53.delete_hosted_zone(Id=zone_id)

    def test_list_hosted_zones(self, route53, hosted_zone):
        response = route53.list_hosted_zones()
        ids = [z["Id"].split("/")[-1] for z in response["HostedZones"]]
        assert hosted_zone in ids

    def test_get_hosted_zone(self, route53, hosted_zone):
        response = route53.get_hosted_zone(Id=hosted_zone)
        assert "example.com." in response["HostedZone"]["Name"]

    def test_change_resource_record_sets(self, route53):
        zone = route53.create_hosted_zone(Name="records.example.com", CallerReference="ref-records")
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        route53.change_resource_record_sets(
            HostedZoneId=zone_id,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "test.records.example.com",
                            "Type": "A",
                            "TTL": 300,
                            "ResourceRecords": [{"Value": "1.2.3.4"}],
                        },
                    }
                ]
            },
        )
        response = route53.list_resource_record_sets(HostedZoneId=zone_id)
        names = [r["Name"] for r in response["ResourceRecordSets"]]
        assert "test.records.example.com." in names
        # Cleanup: delete the record then the zone
        route53.change_resource_record_sets(
            HostedZoneId=zone_id,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": "test.records.example.com",
                            "Type": "A",
                            "TTL": 300,
                            "ResourceRecords": [{"Value": "1.2.3.4"}],
                        },
                    }
                ]
            },
        )
        route53.delete_hosted_zone(Id=zone_id)

    def test_list_resource_record_sets_default(self, route53, hosted_zone):
        """Default hosted zone should contain SOA and NS records."""
        response = route53.list_resource_record_sets(HostedZoneId=hosted_zone)
        assert "ResourceRecordSets" in response
        types = [r["Type"] for r in response["ResourceRecordSets"]]
        assert "SOA" in types
        assert "NS" in types

    def test_get_hosted_zone_includes_nameservers(self, route53, hosted_zone):
        response = route53.get_hosted_zone(Id=hosted_zone)
        assert "DelegationSet" in response
        assert "NameServers" in response["DelegationSet"]
        assert len(response["DelegationSet"]["NameServers"]) >= 1

    def test_create_a_record(self, route53, hosted_zone):
        """Create an A record and verify it appears in record sets."""
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "a-record.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "10.0.0.1"}],
                        },
                    }
                ]
            },
        )
        response = route53.list_resource_record_sets(HostedZoneId=hosted_zone)
        a_records = [
            r
            for r in response["ResourceRecordSets"]
            if r["Type"] == "A" and "a-record.example.com." in r["Name"]
        ]
        assert len(a_records) == 1
        assert a_records[0]["ResourceRecords"][0]["Value"] == "10.0.0.1"
        # Cleanup
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": "a-record.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "10.0.0.1"}],
                        },
                    }
                ]
            },
        )

    def test_create_cname_record(self, route53, hosted_zone):
        """Create a CNAME record and verify."""
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "alias.example.com",
                            "Type": "CNAME",
                            "TTL": 300,
                            "ResourceRecords": [{"Value": "target.example.com"}],
                        },
                    }
                ]
            },
        )
        response = route53.list_resource_record_sets(HostedZoneId=hosted_zone)
        cnames = [r for r in response["ResourceRecordSets"] if r["Type"] == "CNAME"]
        assert len(cnames) >= 1
        assert any("alias.example.com." in c["Name"] for c in cnames)
        # Cleanup
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": "alias.example.com",
                            "Type": "CNAME",
                            "TTL": 300,
                            "ResourceRecords": [{"Value": "target.example.com"}],
                        },
                    }
                ]
            },
        )

    def test_list_resource_record_sets_with_type_filter(self, route53, hosted_zone):
        """List record sets and filter by starting name/type."""
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "web.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "192.168.1.1"}],
                        },
                    }
                ]
            },
        )
        response = route53.list_resource_record_sets(
            HostedZoneId=hosted_zone,
            StartRecordName="web.example.com",
            StartRecordType="A",
        )
        assert "ResourceRecordSets" in response
        names = [r["Name"] for r in response["ResourceRecordSets"]]
        assert "web.example.com." in names
        # Cleanup
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": "web.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "192.168.1.1"}],
                        },
                    }
                ]
            },
        )

    def test_delete_record_set(self, route53, hosted_zone):
        """Create and then delete a record set, verify it is gone."""
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "delete-me.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "1.1.1.1"}],
                        },
                    }
                ]
            },
        )
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": "delete-me.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "1.1.1.1"}],
                        },
                    }
                ]
            },
        )
        response = route53.list_resource_record_sets(HostedZoneId=hosted_zone)
        names = [r["Name"] for r in response["ResourceRecordSets"]]
        assert "delete-me.example.com." not in names

    def test_upsert_record_set(self, route53, hosted_zone):
        """UPSERT should create a record if it doesn't exist, then update it."""
        # First UPSERT creates the record
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": "upsert.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "1.1.1.1"}],
                        },
                    }
                ]
            },
        )
        response = route53.list_resource_record_sets(HostedZoneId=hosted_zone)
        records = [
            r
            for r in response["ResourceRecordSets"]
            if r["Type"] == "A" and "upsert.example.com." in r["Name"]
        ]
        assert len(records) == 1
        assert records[0]["ResourceRecords"][0]["Value"] == "1.1.1.1"

        # Second UPSERT updates the record
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": "upsert.example.com",
                            "Type": "A",
                            "TTL": 120,
                            "ResourceRecords": [{"Value": "2.2.2.2"}],
                        },
                    }
                ]
            },
        )
        response2 = route53.list_resource_record_sets(HostedZoneId=hosted_zone)
        records2 = [
            r
            for r in response2["ResourceRecordSets"]
            if r["Type"] == "A" and "upsert.example.com." in r["Name"]
        ]
        assert len(records2) == 1
        assert records2[0]["ResourceRecords"][0]["Value"] == "2.2.2.2"

        # Cleanup
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": "upsert.example.com",
                            "Type": "A",
                            "TTL": 120,
                            "ResourceRecords": [{"Value": "2.2.2.2"}],
                        },
                    }
                ]
            },
        )

    def test_multiple_record_sets_in_zone(self, route53, hosted_zone):
        """Create multiple record sets and verify they all appear."""
        records_to_create = [
            ("multi-a.example.com", "A", [{"Value": "10.0.0.1"}]),
            ("multi-b.example.com", "A", [{"Value": "10.0.0.2"}]),
            ("multi-c.example.com", "CNAME", [{"Value": "target.example.com"}]),
        ]
        for name, rtype, values in records_to_create:
            route53.change_resource_record_sets(
                HostedZoneId=hosted_zone,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "CREATE",
                            "ResourceRecordSet": {
                                "Name": name,
                                "Type": rtype,
                                "TTL": 300,
                                "ResourceRecords": values,
                            },
                        }
                    ]
                },
            )

        response = route53.list_resource_record_sets(HostedZoneId=hosted_zone)
        names = [r["Name"] for r in response["ResourceRecordSets"]]
        assert "multi-a.example.com." in names
        assert "multi-b.example.com." in names
        assert "multi-c.example.com." in names

        # Cleanup
        for name, rtype, values in records_to_create:
            route53.change_resource_record_sets(
                HostedZoneId=hosted_zone,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "DELETE",
                            "ResourceRecordSet": {
                                "Name": name,
                                "Type": rtype,
                                "TTL": 300,
                                "ResourceRecords": values,
                            },
                        }
                    ]
                },
            )

    def test_create_mx_record(self, route53, hosted_zone):
        """Create an MX record and verify."""
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "example.com",
                            "Type": "MX",
                            "TTL": 300,
                            "ResourceRecords": [{"Value": "10 mail.example.com"}],
                        },
                    }
                ]
            },
        )
        response = route53.list_resource_record_sets(HostedZoneId=hosted_zone)
        mx_records = [r for r in response["ResourceRecordSets"] if r["Type"] == "MX"]
        assert len(mx_records) >= 1
        assert mx_records[0]["ResourceRecords"][0]["Value"] == "10 mail.example.com"
        # Cleanup
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": "example.com",
                            "Type": "MX",
                            "TTL": 300,
                            "ResourceRecords": [{"Value": "10 mail.example.com"}],
                        },
                    }
                ]
            },
        )

    def test_create_txt_record(self, route53, hosted_zone):
        """Create a TXT record and verify."""
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "example.com",
                            "Type": "TXT",
                            "TTL": 300,
                            "ResourceRecords": [{"Value": '"v=spf1 include:example.com ~all"'}],
                        },
                    }
                ]
            },
        )
        response = route53.list_resource_record_sets(HostedZoneId=hosted_zone)
        txt_records = [r for r in response["ResourceRecordSets"] if r["Type"] == "TXT"]
        assert len(txt_records) >= 1
        # Cleanup
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": "example.com",
                            "Type": "TXT",
                            "TTL": 300,
                            "ResourceRecords": [{"Value": '"v=spf1 include:example.com ~all"'}],
                        },
                    }
                ]
            },
        )


class TestRoute53Tags:
    def test_change_and_list_tags(self, route53, hosted_zone):
        """Tag a hosted zone and verify tags."""
        route53.change_tags_for_resource(
            ResourceType="hostedzone",
            ResourceId=hosted_zone,
            AddTags=[
                {"Key": "Environment", "Value": "test"},
                {"Key": "Project", "Value": "robotocore"},
            ],
        )
        response = route53.list_tags_for_resource(
            ResourceType="hostedzone",
            ResourceId=hosted_zone,
        )
        tags = {t["Key"]: t["Value"] for t in response["ResourceTagSet"]["Tags"]}
        assert tags["Environment"] == "test"
        assert tags["Project"] == "robotocore"

    def test_remove_tags(self, route53, hosted_zone):
        """Add and then remove a tag."""
        route53.change_tags_for_resource(
            ResourceType="hostedzone",
            ResourceId=hosted_zone,
            AddTags=[{"Key": "ToRemove", "Value": "yes"}],
        )
        route53.change_tags_for_resource(
            ResourceType="hostedzone",
            ResourceId=hosted_zone,
            RemoveTagKeys=["ToRemove"],
        )
        response = route53.list_tags_for_resource(
            ResourceType="hostedzone",
            ResourceId=hosted_zone,
        )
        keys = [t["Key"] for t in response["ResourceTagSet"].get("Tags", [])]
        assert "ToRemove" not in keys


class TestRoute53ZoneQueries:
    def test_get_hosted_zone_count(self, route53, hosted_zone):
        """GetHostedZoneCount should return at least 1."""
        response = route53.get_hosted_zone_count()
        assert response["HostedZoneCount"] >= 1

    def test_list_hosted_zones_by_name(self, route53, hosted_zone):
        """ListHostedZonesByName should return zones."""
        response = route53.list_hosted_zones_by_name()
        assert "HostedZones" in response
        assert len(response["HostedZones"]) >= 1
        names = [z["Name"] for z in response["HostedZones"]]
        assert "example.com." in names

    def test_list_hosted_zones_by_name_with_dns_name(self, route53, hosted_zone):
        """ListHostedZonesByName can filter by DNSName."""
        response = route53.list_hosted_zones_by_name(DNSName="example.com")
        assert "HostedZones" in response
        assert len(response["HostedZones"]) >= 1

    def test_create_hosted_zone_with_comment(self, route53):
        """Create a hosted zone with a comment and verify."""
        response = route53.create_hosted_zone(
            Name="commented.example.com",
            CallerReference="commented-ref-1",
            HostedZoneConfig={
                "Comment": "Test zone with comment",
                "PrivateZone": False,
            },
        )
        zone_id = response["HostedZone"]["Id"].split("/")[-1]
        try:
            got = route53.get_hosted_zone(Id=zone_id)
            assert "commented.example.com." in got["HostedZone"]["Name"]
        finally:
            route53.delete_hosted_zone(Id=zone_id)

    def test_change_resource_record_sets_returns_change_info(self, route53, hosted_zone):
        """ChangeResourceRecordSets returns a ChangeInfo with Id and Status."""
        response = route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "changeinfo.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "5.5.5.5"}],
                        },
                    }
                ]
            },
        )
        assert "ChangeInfo" in response
        assert "Id" in response["ChangeInfo"]
        assert "Status" in response["ChangeInfo"]
        # Cleanup
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": "changeinfo.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "5.5.5.5"}],
                        },
                    }
                ]
            },
        )


class TestRoute53Extended:
    def test_get_hosted_zone_details(self, route53):
        """CreateHostedZone with HostedZoneConfig, then GetHostedZone to verify details."""
        caller_ref = uuid.uuid4().hex[:8]
        response = route53.create_hosted_zone(
            Name="details.example.com",
            CallerReference=caller_ref,
            HostedZoneConfig={
                "Comment": "Zone with details",
                "PrivateZone": False,
            },
        )
        zone_id = response["HostedZone"]["Id"].split("/")[-1]
        try:
            got = route53.get_hosted_zone(Id=zone_id)
            assert "details.example.com." in got["HostedZone"]["Name"]
            assert "Config" in got["HostedZone"]
            assert got["HostedZone"]["Config"]["Comment"] == "Zone with details"
            assert got["HostedZone"]["Config"]["PrivateZone"] is False
            assert "DelegationSet" in got
        finally:
            route53.delete_hosted_zone(Id=zone_id)

    def test_list_health_checks(self, route53):
        """Create a health check, list health checks, verify ID present, then clean up."""
        caller_ref = uuid.uuid4().hex[:8]
        create_resp = route53.create_health_check(
            CallerReference=caller_ref,
            HealthCheckConfig={
                "Type": "HTTP",
                "FullyQualifiedDomainName": "healthcheck.example.com",
                "Port": 80,
                "ResourcePath": "/",
            },
        )
        hc_id = create_resp["HealthCheck"]["Id"]
        try:
            list_resp = route53.list_health_checks()
            hc_ids = [hc["Id"] for hc in list_resp["HealthChecks"]]
            assert hc_id in hc_ids
        finally:
            route53.delete_health_check(HealthCheckId=hc_id)

    def test_get_hosted_zone_returns_all_fields(self, route53, hosted_zone):
        """GetHostedZone returns HostedZone, DelegationSet, and expected sub-fields."""
        resp = route53.get_hosted_zone(Id=hosted_zone)
        hz = resp["HostedZone"]
        assert "Id" in hz
        assert "Name" in hz
        assert "CallerReference" in hz
        assert "Config" in hz
        assert "ResourceRecordSetCount" in hz
        assert "DelegationSet" in resp

    def test_create_health_check_http(self, route53):
        """CreateHealthCheck with HTTP type."""
        ref = _unique("hc-http")
        resp = route53.create_health_check(
            CallerReference=ref,
            HealthCheckConfig={
                "IPAddress": "1.2.3.4",
                "Port": 80,
                "Type": "HTTP",
                "ResourcePath": "/health",
                "RequestInterval": 30,
                "FailureThreshold": 3,
            },
        )
        hc_id = resp["HealthCheck"]["Id"]
        try:
            assert resp["HealthCheck"]["HealthCheckConfig"]["Type"] == "HTTP"
        finally:
            route53.delete_health_check(HealthCheckId=hc_id)

    def test_create_health_check_https(self, route53):
        """CreateHealthCheck with HTTPS type."""
        ref = _unique("hc-https")
        resp = route53.create_health_check(
            CallerReference=ref,
            HealthCheckConfig={
                "IPAddress": "1.2.3.4",
                "Port": 443,
                "Type": "HTTPS",
                "ResourcePath": "/",
                "RequestInterval": 30,
                "FailureThreshold": 3,
            },
        )
        hc_id = resp["HealthCheck"]["Id"]
        try:
            assert resp["HealthCheck"]["HealthCheckConfig"]["Type"] == "HTTPS"
        finally:
            route53.delete_health_check(HealthCheckId=hc_id)

    def test_create_health_check_tcp(self, route53):
        """CreateHealthCheck with TCP type."""
        ref = _unique("hc-tcp")
        resp = route53.create_health_check(
            CallerReference=ref,
            HealthCheckConfig={
                "IPAddress": "1.2.3.4",
                "Port": 5432,
                "Type": "TCP",
                "RequestInterval": 30,
                "FailureThreshold": 3,
            },
        )
        hc_id = resp["HealthCheck"]["Id"]
        try:
            assert resp["HealthCheck"]["HealthCheckConfig"]["Type"] == "TCP"
        finally:
            route53.delete_health_check(HealthCheckId=hc_id)

    def test_get_health_check(self, route53):
        """GetHealthCheck / DeleteHealthCheck."""
        ref = _unique("hc-get")
        create_resp = route53.create_health_check(
            CallerReference=ref,
            HealthCheckConfig={
                "IPAddress": "10.0.0.1",
                "Port": 80,
                "Type": "HTTP",
                "RequestInterval": 30,
                "FailureThreshold": 3,
            },
        )
        hc_id = create_resp["HealthCheck"]["Id"]
        try:
            resp = route53.get_health_check(HealthCheckId=hc_id)
            assert resp["HealthCheck"]["Id"] == hc_id
            assert resp["HealthCheck"]["HealthCheckConfig"]["IPAddress"] == "10.0.0.1"
        finally:
            route53.delete_health_check(HealthCheckId=hc_id)

    def test_weighted_routing(self, route53):
        """ChangeResourceRecordSets with weighted routing."""
        ref = _unique("weighted")
        zone = route53.create_hosted_zone(Name="weighted.example.com", CallerReference=ref)
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "CREATE",
                            "ResourceRecordSet": {
                                "Name": "app.weighted.example.com",
                                "Type": "A",
                                "SetIdentifier": "weight-1",
                                "Weight": 70,
                                "TTL": 60,
                                "ResourceRecords": [{"Value": "10.0.0.1"}],
                            },
                        },
                        {
                            "Action": "CREATE",
                            "ResourceRecordSet": {
                                "Name": "app.weighted.example.com",
                                "Type": "A",
                                "SetIdentifier": "weight-2",
                                "Weight": 30,
                                "TTL": 60,
                                "ResourceRecords": [{"Value": "10.0.0.2"}],
                            },
                        },
                    ]
                },
            )
            resp = route53.list_resource_record_sets(HostedZoneId=zone_id)
            weighted = [
                r
                for r in resp["ResourceRecordSets"]
                if r.get("SetIdentifier") in ("weight-1", "weight-2")
            ]
            assert len(weighted) == 2
            # Cleanup
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "DELETE",
                            "ResourceRecordSet": {
                                "Name": "app.weighted.example.com",
                                "Type": "A",
                                "SetIdentifier": "weight-1",
                                "Weight": 70,
                                "TTL": 60,
                                "ResourceRecords": [{"Value": "10.0.0.1"}],
                            },
                        },
                        {
                            "Action": "DELETE",
                            "ResourceRecordSet": {
                                "Name": "app.weighted.example.com",
                                "Type": "A",
                                "SetIdentifier": "weight-2",
                                "Weight": 30,
                                "TTL": 60,
                                "ResourceRecords": [{"Value": "10.0.0.2"}],
                            },
                        },
                    ]
                },
            )
        finally:
            route53.delete_hosted_zone(Id=zone_id)

    def test_alias_records(self, route53):
        """ChangeResourceRecordSets with alias records."""
        ref = _unique("alias")
        zone = route53.create_hosted_zone(Name="alias.example.com", CallerReference=ref)
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            # Create an A record first to alias to
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "CREATE",
                            "ResourceRecordSet": {
                                "Name": "target.alias.example.com",
                                "Type": "A",
                                "TTL": 60,
                                "ResourceRecords": [{"Value": "10.0.0.1"}],
                            },
                        },
                        {
                            "Action": "CREATE",
                            "ResourceRecordSet": {
                                "Name": "www.alias.example.com",
                                "Type": "A",
                                "AliasTarget": {
                                    "HostedZoneId": zone_id,
                                    "DNSName": "target.alias.example.com",
                                    "EvaluateTargetHealth": False,
                                },
                            },
                        },
                    ]
                },
            )
            resp = route53.list_resource_record_sets(HostedZoneId=zone_id)
            alias_records = [
                r
                for r in resp["ResourceRecordSets"]
                if r["Name"] == "www.alias.example.com." and "AliasTarget" in r
            ]
            assert len(alias_records) == 1
            assert (
                alias_records[0]["AliasTarget"]["DNSName"].rstrip(".") == "target.alias.example.com"
            )
            # Cleanup
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "DELETE",
                            "ResourceRecordSet": {
                                "Name": "www.alias.example.com",
                                "Type": "A",
                                "AliasTarget": {
                                    "HostedZoneId": zone_id,
                                    "DNSName": "target.alias.example.com",
                                    "EvaluateTargetHealth": False,
                                },
                            },
                        },
                        {
                            "Action": "DELETE",
                            "ResourceRecordSet": {
                                "Name": "target.alias.example.com",
                                "Type": "A",
                                "TTL": 60,
                                "ResourceRecords": [{"Value": "10.0.0.1"}],
                            },
                        },
                    ]
                },
            )
        finally:
            try:
                route53.delete_hosted_zone(Id=zone_id)
            except Exception:
                # Zone might have records; try to clean up
                try:
                    resp = route53.list_resource_record_sets(HostedZoneId=zone_id)
                    changes = []
                    for r in resp["ResourceRecordSets"]:
                        if r["Type"] not in ("SOA", "NS"):
                            changes.append({"Action": "DELETE", "ResourceRecordSet": r})
                    if changes:
                        route53.change_resource_record_sets(
                            HostedZoneId=zone_id,
                            ChangeBatch={"Changes": changes},
                        )
                    route53.delete_hosted_zone(Id=zone_id)
                except Exception:
                    pass

    def test_list_resource_record_sets_pagination(self, route53):
        """ListResourceRecordSets with MaxItems pagination."""
        ref = _unique("paginate")
        zone = route53.create_hosted_zone(Name="paginate.example.com", CallerReference=ref)
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            # Create several records
            changes = []
            for i in range(5):
                changes.append(
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": f"r{i}.paginate.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": f"10.0.0.{i + 1}"}],
                        },
                    }
                )
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={"Changes": changes},
            )
            # Request with MaxItems=2 (SOA+NS+5 custom = 7 total)
            resp = route53.list_resource_record_sets(
                HostedZoneId=zone_id,
                MaxItems="2",
            )
            assert len(resp["ResourceRecordSets"]) <= 2
            assert resp["IsTruncated"] is True

            # Cleanup
            del_changes = []
            for i in range(5):
                del_changes.append(
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": f"r{i}.paginate.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": f"10.0.0.{i + 1}"}],
                        },
                    }
                )
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={"Changes": del_changes},
            )
        finally:
            route53.delete_hosted_zone(Id=zone_id)

    def test_change_tags_for_resource(self, route53, hosted_zone):
        """ChangeTagsForResource / ListTagsForResource on hosted zones."""
        route53.change_tags_for_resource(
            ResourceType="hostedzone",
            ResourceId=hosted_zone,
            AddTags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        resp = route53.list_tags_for_resource(
            ResourceType="hostedzone",
            ResourceId=hosted_zone,
        )
        tags = {t["Key"]: t["Value"] for t in resp["ResourceTagSet"]["Tags"]}
        assert tags["env"] == "test"
        assert tags["team"] == "platform"

        # Remove one tag
        route53.change_tags_for_resource(
            ResourceType="hostedzone",
            ResourceId=hosted_zone,
            RemoveTagKeys=["team"],
        )
        resp = route53.list_tags_for_resource(
            ResourceType="hostedzone",
            ResourceId=hosted_zone,
        )
        tags = {t["Key"]: t["Value"] for t in resp["ResourceTagSet"]["Tags"]}
        assert "team" not in tags
        assert tags["env"] == "test"

    def test_associate_disassociate_vpc(self, route53):
        """AssociateVPCWithHostedZone / DisassociateVPCFromHostedZone."""
        ec2 = make_client("ec2")
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        ref = _unique("vpc-assoc")
        zone = route53.create_hosted_zone(
            Name="private.example.com",
            CallerReference=ref,
            HostedZoneConfig={"PrivateZone": True},
            VPC={"VPCRegion": "us-east-1", "VPCId": vpc_id},
        )
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            # Create a second VPC to associate
            vpc2 = ec2.create_vpc(CidrBlock="10.1.0.0/16")
            vpc2_id = vpc2["Vpc"]["VpcId"]
            route53.associate_vpc_with_hosted_zone(
                HostedZoneId=zone_id,
                VPC={"VPCRegion": "us-east-1", "VPCId": vpc2_id},
            )
            route53.disassociate_vpc_from_hosted_zone(
                HostedZoneId=zone_id,
                VPC={"VPCRegion": "us-east-1", "VPCId": vpc2_id},
            )
            ec2.delete_vpc(VpcId=vpc2_id)
        finally:
            route53.delete_hosted_zone(Id=zone_id)
            ec2.delete_vpc(VpcId=vpc_id)

    def test_dns_answer(self, route53, hosted_zone):
        """TestDNSAnswer."""
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "dns-test.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "10.0.0.1"}],
                        },
                    }
                ]
            },
        )
        try:
            resp = route53.test_dns_answer(
                HostedZoneId=hosted_zone,
                RecordName="dns-test.example.com",
                RecordType="A",
            )
            assert "RecordData" in resp
        finally:
            route53.change_resource_record_sets(
                HostedZoneId=hosted_zone,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "DELETE",
                            "ResourceRecordSet": {
                                "Name": "dns-test.example.com",
                                "Type": "A",
                                "TTL": 60,
                                "ResourceRecords": [{"Value": "10.0.0.1"}],
                            },
                        }
                    ]
                },
            )

    def test_create_query_logging_config(self, route53, hosted_zone):
        """CreateQueryLoggingConfig."""
        logs = make_client("logs")
        log_group = f"/aws/route53/{_unique('qlc')}"
        logs.create_log_group(logGroupName=log_group)
        try:
            resp = route53.create_query_logging_config(
                HostedZoneId=hosted_zone,
                CloudWatchLogsLogGroupArn=f"arn:aws:logs:us-east-1:123456789012:log-group:{log_group}",
            )
            assert "QueryLoggingConfig" in resp
        finally:
            logs.delete_log_group(logGroupName=log_group)

    def test_get_query_logging_config_nonexistent(self, route53):
        """GetQueryLoggingConfig returns error for nonexistent config."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            route53.get_query_logging_config(Id="00000000-0000-0000-0000-000000000000")
        assert exc.value.response["Error"]["Code"] == "NoSuchQueryLoggingConfig"

    def test_delete_query_logging_config_nonexistent(self, route53):
        """DeleteQueryLoggingConfig returns error for nonexistent config."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            route53.delete_query_logging_config(Id="00000000-0000-0000-0000-000000000000")
        assert exc.value.response["Error"]["Code"] == "NoSuchQueryLoggingConfig"


class TestRoute53ExtendedV2:
    @pytest.fixture
    def route53(self):
        return make_client("route53")

    def test_create_private_hosted_zone(self, route53):
        ec2 = make_client("ec2")
        vpc = ec2.create_vpc(CidrBlock="10.99.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        ref = _unique("private")
        try:
            zone = route53.create_hosted_zone(
                Name="private.test.com",
                CallerReference=ref,
                HostedZoneConfig={"PrivateZone": True},
                VPC={"VPCRegion": "us-east-1", "VPCId": vpc_id},
            )
            zone_id = zone["HostedZone"]["Id"].split("/")[-1]
            assert zone["HostedZone"]["Config"]["PrivateZone"] is True
            route53.delete_hosted_zone(Id=zone_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_create_aaaa_record(self, route53):
        ref = _unique("aaaa")
        zone = route53.create_hosted_zone(Name="aaaa.test.com", CallerReference=ref)
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "CREATE",
                            "ResourceRecordSet": {
                                "Name": "ipv6.aaaa.test.com",
                                "Type": "AAAA",
                                "TTL": 300,
                                "ResourceRecords": [{"Value": "2001:db8::1"}],
                            },
                        }
                    ]
                },
            )
            resp = route53.list_resource_record_sets(HostedZoneId=zone_id)
            aaaa = [r for r in resp["ResourceRecordSets"] if r["Type"] == "AAAA"]
            assert len(aaaa) == 1
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "DELETE",
                            "ResourceRecordSet": aaaa[0],
                        }
                    ]
                },
            )
        finally:
            route53.delete_hosted_zone(Id=zone_id)

    def test_create_srv_record(self, route53):
        ref = _unique("srv")
        zone = route53.create_hosted_zone(Name="srv.test.com", CallerReference=ref)
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "CREATE",
                            "ResourceRecordSet": {
                                "Name": "_sip._tcp.srv.test.com",
                                "Type": "SRV",
                                "TTL": 60,
                                "ResourceRecords": [{"Value": "10 60 5060 sip.srv.test.com"}],
                            },
                        }
                    ]
                },
            )
            resp = route53.list_resource_record_sets(HostedZoneId=zone_id)
            srv = [r for r in resp["ResourceRecordSets"] if r["Type"] == "SRV"]
            assert len(srv) == 1
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={"Changes": [{"Action": "DELETE", "ResourceRecordSet": srv[0]}]},
            )
        finally:
            route53.delete_hosted_zone(Id=zone_id)

    def test_multiple_records_same_type(self, route53):
        ref = _unique("multi-a")
        zone = route53.create_hosted_zone(Name="multi.test.com", CallerReference=ref)
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "CREATE",
                            "ResourceRecordSet": {
                                "Name": "app.multi.test.com",
                                "Type": "A",
                                "TTL": 60,
                                "ResourceRecords": [
                                    {"Value": "10.0.0.1"},
                                    {"Value": "10.0.0.2"},
                                    {"Value": "10.0.0.3"},
                                ],
                            },
                        }
                    ]
                },
            )
            resp = route53.list_resource_record_sets(HostedZoneId=zone_id)
            a_recs = [
                r
                for r in resp["ResourceRecordSets"]
                if r["Name"].startswith("app.multi") and r["Type"] == "A"
            ]
            assert len(a_recs) == 1
            assert len(a_recs[0]["ResourceRecords"]) == 3
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={"Changes": [{"Action": "DELETE", "ResourceRecordSet": a_recs[0]}]},
            )
        finally:
            route53.delete_hosted_zone(Id=zone_id)

    def test_upsert_creates_and_updates(self, route53):
        ref = _unique("upsert2")
        zone = route53.create_hosted_zone(Name="upsert2.test.com", CallerReference=ref)
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            rrs = {
                "Name": "www.upsert2.test.com",
                "Type": "A",
                "TTL": 60,
                "ResourceRecords": [{"Value": "1.1.1.1"}],
            }
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={"Changes": [{"Action": "UPSERT", "ResourceRecordSet": rrs}]},
            )
            rrs["ResourceRecords"] = [{"Value": "2.2.2.2"}]
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={"Changes": [{"Action": "UPSERT", "ResourceRecordSet": rrs}]},
            )
            resp = route53.list_resource_record_sets(HostedZoneId=zone_id)
            www = [
                r
                for r in resp["ResourceRecordSets"]
                if r["Name"].startswith("www.upsert2") and r["Type"] == "A"
            ]
            assert len(www) == 1
            assert www[0]["ResourceRecords"][0]["Value"] == "2.2.2.2"
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={"Changes": [{"Action": "DELETE", "ResourceRecordSet": rrs}]},
            )
        finally:
            route53.delete_hosted_zone(Id=zone_id)

    def test_hosted_zone_has_soa_and_ns(self, route53):
        ref = _unique("soans")
        zone = route53.create_hosted_zone(Name="soans.test.com", CallerReference=ref)
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            resp = route53.list_resource_record_sets(HostedZoneId=zone_id)
            types = {r["Type"] for r in resp["ResourceRecordSets"]}
            assert "SOA" in types
            assert "NS" in types
        finally:
            route53.delete_hosted_zone(Id=zone_id)

    def test_get_change(self, route53):
        ref = _unique("chg")
        zone = route53.create_hosted_zone(Name="chg.test.com", CallerReference=ref)
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            change = route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "CREATE",
                            "ResourceRecordSet": {
                                "Name": "x.chg.test.com",
                                "Type": "A",
                                "TTL": 60,
                                "ResourceRecords": [{"Value": "3.3.3.3"}],
                            },
                        }
                    ]
                },
            )
            change_id = change["ChangeInfo"]["Id"].split("/")[-1]
            resp = route53.get_change(Id=change_id)
            assert resp["ChangeInfo"]["Status"] in ("PENDING", "INSYNC")
            route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "DELETE",
                            "ResourceRecordSet": {
                                "Name": "x.chg.test.com",
                                "Type": "A",
                                "TTL": 60,
                                "ResourceRecords": [{"Value": "3.3.3.3"}],
                            },
                        }
                    ]
                },
            )
        finally:
            route53.delete_hosted_zone(Id=zone_id)

    def test_create_multiple_hosted_zones(self, route53):
        zones = []
        for i in range(3):
            ref = _unique(f"multi-zone-{i}")
            z = route53.create_hosted_zone(Name=f"zone{i}.test.com", CallerReference=ref)
            zones.append(z["HostedZone"]["Id"].split("/")[-1])
        try:
            resp = route53.list_hosted_zones()
            ids = {z["Id"].split("/")[-1] for z in resp["HostedZones"]}
            for zid in zones:
                assert zid in ids
        finally:
            for zid in zones:
                route53.delete_hosted_zone(Id=zid)

    def test_hosted_zone_record_set_count(self, route53):
        ref = _unique("count")
        zone = route53.create_hosted_zone(Name="count.test.com", CallerReference=ref)
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            resp = route53.get_hosted_zone(Id=zone_id)
            # SOA + NS = at least 2
            assert resp["HostedZone"]["ResourceRecordSetCount"] >= 2
        finally:
            route53.delete_hosted_zone(Id=zone_id)

    def test_delete_hosted_zone_with_no_custom_records(self, route53):
        ref = _unique("delzone")
        zone = route53.create_hosted_zone(Name="delzone.test.com", CallerReference=ref)
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        route53.delete_hosted_zone(Id=zone_id)
        resp = route53.list_hosted_zones()
        ids = {z["Id"].split("/")[-1] for z in resp["HostedZones"]}
        assert zone_id not in ids

    def test_list_reusable_delegation_sets(self, route53):
        """ListReusableDelegationSets returns a list (may be empty)."""
        resp = route53.list_reusable_delegation_sets()
        assert "DelegationSets" in resp
        assert isinstance(resp["DelegationSets"], list)

    def test_list_tags_for_resources(self, route53):
        """ListTagsForResources (plural) returns tags for multiple hosted zones."""
        ref1 = _unique("tags-res-1")
        ref2 = _unique("tags-res-2")
        zone1 = route53.create_hosted_zone(Name="tags1.test.com", CallerReference=ref1)
        zone2 = route53.create_hosted_zone(Name="tags2.test.com", CallerReference=ref2)
        z1_id = zone1["HostedZone"]["Id"].split("/")[-1]
        z2_id = zone2["HostedZone"]["Id"].split("/")[-1]
        try:
            route53.change_tags_for_resource(
                ResourceType="hostedzone",
                ResourceId=z1_id,
                AddTags=[{"Key": "env", "Value": "test"}],
            )
            resp = route53.list_tags_for_resources(
                ResourceType="hostedzone",
                ResourceIds=[z1_id, z2_id],
            )
            assert "ResourceTagSets" in resp
            assert len(resp["ResourceTagSets"]) == 2
        finally:
            route53.delete_hosted_zone(Id=z1_id)
            route53.delete_hosted_zone(Id=z2_id)

    def test_update_health_check(self, route53):
        """UpdateHealthCheck modifies an existing health check."""
        resp = route53.create_health_check(
            CallerReference=_unique("upd-hc"),
            HealthCheckConfig={
                "Type": "HTTP",
                "FullyQualifiedDomainName": "update.example.com",
                "Port": 80,
                "ResourcePath": "/health",
            },
        )
        hc_id = resp["HealthCheck"]["Id"]
        try:
            update_resp = route53.update_health_check(
                HealthCheckId=hc_id,
                ResourcePath="/updated-health",
                Port=8080,
            )
            assert update_resp["HealthCheck"]["Id"] == hc_id
            config = update_resp["HealthCheck"]["HealthCheckConfig"]
            assert config["ResourcePath"] == "/updated-health"
            assert config["Port"] == 8080
        finally:
            route53.delete_health_check(HealthCheckId=hc_id)

    def test_delete_health_check(self, route53):
        """Create and delete a health check, verify it's gone."""
        resp = route53.create_health_check(
            CallerReference=_unique("del-hc"),
            HealthCheckConfig={
                "Type": "TCP",
                "IPAddress": "10.0.0.1",
                "Port": 443,
            },
        )
        hc_id = resp["HealthCheck"]["Id"]
        route53.delete_health_check(HealthCheckId=hc_id)
        checks = route53.list_health_checks()
        hc_ids = [h["Id"] for h in checks["HealthChecks"]]
        assert hc_id not in hc_ids

    def test_list_tags_for_resource_health_check(self, route53):
        """ListTagsForResource works for health checks too."""
        resp = route53.create_health_check(
            CallerReference=_unique("tag-hc"),
            HealthCheckConfig={
                "Type": "HTTP",
                "FullyQualifiedDomainName": "tags.example.com",
                "Port": 80,
            },
        )
        hc_id = resp["HealthCheck"]["Id"]
        try:
            route53.change_tags_for_resource(
                ResourceType="healthcheck",
                ResourceId=hc_id,
                AddTags=[{"Key": "env", "Value": "staging"}],
            )
            tag_resp = route53.list_tags_for_resource(
                ResourceType="healthcheck",
                ResourceId=hc_id,
            )
            tags = tag_resp["ResourceTagSet"]["Tags"]
            assert any(t["Key"] == "env" and t["Value"] == "staging" for t in tags)
        finally:
            route53.delete_health_check(HealthCheckId=hc_id)


# ---------------------------------------------------------------------------
# Gap stubs — newly verified operations
# ---------------------------------------------------------------------------


class TestRoute53GapStubs:
    def test_get_checker_ip_ranges(self, route53):
        """GetCheckerIpRanges returns a list of IP ranges."""
        resp = route53.get_checker_ip_ranges()
        assert "CheckerIpRanges" in resp
        assert isinstance(resp["CheckerIpRanges"], list)

    def test_get_geo_location(self, route53):
        """GetGeoLocation returns geo location details."""
        resp = route53.get_geo_location()
        assert "GeoLocationDetails" in resp

    def test_get_health_check_count(self, route53):
        """GetHealthCheckCount returns a count."""
        resp = route53.get_health_check_count()
        assert "HealthCheckCount" in resp
        assert isinstance(resp["HealthCheckCount"], int)

    def test_get_traffic_policy_instance_count(self, route53):
        """GetTrafficPolicyInstanceCount returns a count."""
        resp = route53.get_traffic_policy_instance_count()
        assert "TrafficPolicyInstanceCount" in resp
        assert isinstance(resp["TrafficPolicyInstanceCount"], int)

    def test_list_cidr_collections(self, route53):
        """ListCidrCollections returns a list (possibly empty)."""
        resp = route53.list_cidr_collections()
        assert "CidrCollections" in resp
        assert isinstance(resp["CidrCollections"], list)

    def test_list_geo_locations(self, route53):
        """ListGeoLocations returns geo location details."""
        resp = route53.list_geo_locations()
        assert "GeoLocationDetailsList" in resp
        assert isinstance(resp["GeoLocationDetailsList"], list)

    def test_list_traffic_policies(self, route53):
        """ListTrafficPolicies returns a list (possibly empty)."""
        resp = route53.list_traffic_policies()
        assert "TrafficPolicySummaries" in resp
        assert isinstance(resp["TrafficPolicySummaries"], list)

    def test_list_traffic_policy_instances(self, route53):
        """ListTrafficPolicyInstances returns a list (possibly empty)."""
        resp = route53.list_traffic_policy_instances()
        assert "TrafficPolicyInstances" in resp
        assert isinstance(resp["TrafficPolicyInstances"], list)


class TestRoute53ReusableDelegationSets:
    """Tests for reusable delegation set operations."""

    def test_create_and_get_reusable_delegation_set(self, route53):
        """CreateReusableDelegationSet and GetReusableDelegationSet."""
        ref = _unique("ds-ref")
        resp = route53.create_reusable_delegation_set(CallerReference=ref)
        ds = resp["DelegationSet"]
        ds_id = ds["Id"].split("/")[-1]
        assert "NameServers" in ds
        assert len(ds["NameServers"]) > 0
        try:
            get_resp = route53.get_reusable_delegation_set(Id=ds_id)
            assert get_resp["DelegationSet"]["Id"].endswith(ds_id)
            assert "NameServers" in get_resp["DelegationSet"]
        finally:
            route53.delete_reusable_delegation_set(Id=ds_id)

    def test_delete_reusable_delegation_set(self, route53):
        """DeleteReusableDelegationSet removes the delegation set."""
        ref = _unique("ds-del-ref")
        resp = route53.create_reusable_delegation_set(CallerReference=ref)
        ds_id = resp["DelegationSet"]["Id"].split("/")[-1]
        route53.delete_reusable_delegation_set(Id=ds_id)
        # Verify it's gone
        with pytest.raises(Exception):
            route53.get_reusable_delegation_set(Id=ds_id)


class TestRoute53AdditionalOperations:
    """Tests for additional Route53 operations."""

    def test_update_hosted_zone_comment(self, route53):
        """UpdateHostedZoneComment sets a comment on the hosted zone."""
        zone = route53.create_hosted_zone(
            Name="comment-update.example.com",
            CallerReference=_unique("comment-ref"),
        )
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            resp = route53.update_hosted_zone_comment(Id=zone_id, Comment="My updated comment")
            assert resp["HostedZone"]["Config"]["Comment"] == "My updated comment"
            # Verify via get
            get_resp = route53.get_hosted_zone(Id=zone_id)
            assert get_resp["HostedZone"]["Config"]["Comment"] == "My updated comment"
        finally:
            route53.delete_hosted_zone(Id=zone_id)

    def test_list_query_logging_configs(self, route53):
        """ListQueryLoggingConfigs returns a list (possibly empty)."""
        resp = route53.list_query_logging_configs()
        assert "QueryLoggingConfigs" in resp
        assert isinstance(resp["QueryLoggingConfigs"], list)

    def test_get_dnssec(self, route53):
        """GetDNSSEC returns DNSSEC status for a hosted zone."""
        zone = route53.create_hosted_zone(
            Name="dnssec-check.example.com",
            CallerReference=_unique("dnssec-ref"),
        )
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            resp = route53.get_dnssec(HostedZoneId=zone_id)
            assert "Status" in resp
            assert resp["Status"]["ServeSignature"] in ("NOT_SIGNING", "SIGNING")
            assert "KeySigningKeys" in resp
        finally:
            route53.delete_hosted_zone(Id=zone_id)

    def test_list_hosted_zones_by_vpc(self, route53):
        """ListHostedZonesByVPC returns a list of hosted zone summaries."""
        resp = route53.list_hosted_zones_by_vpc(VPCId="vpc-12345678", VPCRegion="us-east-1")
        assert "HostedZoneSummaries" in resp
        assert isinstance(resp["HostedZoneSummaries"], list)

    def test_get_health_check_status(self, route53):
        """GetHealthCheckStatus returns observation data."""
        hc = route53.create_health_check(
            CallerReference=_unique("hc-status-ref"),
            HealthCheckConfig={
                "Type": "HTTP",
                "FullyQualifiedDomainName": "example.com",
                "Port": 80,
                "RequestInterval": 30,
                "FailureThreshold": 3,
            },
        )
        hc_id = hc["HealthCheck"]["Id"]
        try:
            resp = route53.get_health_check_status(HealthCheckId=hc_id)
            assert "HealthCheckObservations" in resp
            assert isinstance(resp["HealthCheckObservations"], list)
        finally:
            route53.delete_health_check(HealthCheckId=hc_id)
