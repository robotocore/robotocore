"""Route53 compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


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
            r for r in response["ResourceRecordSets"]
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
            r for r in response2["ResourceRecordSets"]
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

    def test_get_hosted_zone_count(self, route53):
        """GetHostedZoneCount returns HostedZoneCount as an int."""
        response = route53.get_hosted_zone_count()
        assert "HostedZoneCount" in response
        assert isinstance(response["HostedZoneCount"], int)

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

    def test_create_mx_record(self, route53, hosted_zone):
        """Create an MX record via ChangeResourceRecordSets, list and verify, then clean up."""
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "mx.example.com",
                            "Type": "MX",
                            "TTL": 300,
                            "ResourceRecords": [{"Value": "10 mail.example.com"}],
                        },
                    }
                ]
            },
        )
        try:
            response = route53.list_resource_record_sets(HostedZoneId=hosted_zone)
            mx_records = [r for r in response["ResourceRecordSets"] if r["Type"] == "MX"]
            assert len(mx_records) >= 1
            values = [rr["Value"] for rec in mx_records for rr in rec["ResourceRecords"]]
            assert "10 mail.example.com" in values
        finally:
            route53.change_resource_record_sets(
                HostedZoneId=hosted_zone,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "DELETE",
                            "ResourceRecordSet": {
                                "Name": "mx.example.com",
                                "Type": "MX",
                                "TTL": 300,
                                "ResourceRecords": [{"Value": "10 mail.example.com"}],
                            },
                        }
                    ]
                },
            )

    def test_create_txt_record(self, route53, hosted_zone):
        """Create a TXT record with quoted value, list and verify, then clean up."""
        txt_value = '"v=spf1 include:example.com ~all"'
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "txt.example.com",
                            "Type": "TXT",
                            "TTL": 300,
                            "ResourceRecords": [{"Value": txt_value}],
                        },
                    }
                ]
            },
        )
        try:
            response = route53.list_resource_record_sets(HostedZoneId=hosted_zone)
            txt_records = [r for r in response["ResourceRecordSets"] if r["Type"] == "TXT"]
            assert len(txt_records) >= 1
            values = [rr["Value"] for rec in txt_records for rr in rec["ResourceRecords"]]
            assert txt_value in values
        finally:
            route53.change_resource_record_sets(
                HostedZoneId=hosted_zone,
                ChangeBatch={
                    "Changes": [
                        {
                            "Action": "DELETE",
                            "ResourceRecordSet": {
                                "Name": "txt.example.com",
                                "Type": "TXT",
                                "TTL": 300,
                                "ResourceRecords": [{"Value": txt_value}],
                            },
                        }
                    ]
                },
            )
