"""Route53 compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


def _unique(prefix: str) -> str:
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

    def test_create_txt_record(self, route53, hosted_zone):
        """Create a TXT record and verify it appears."""
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
                            "ResourceRecords": [
                                {"Value": '"v=spf1 include:example.com ~all"'}
                            ],
                        },
                    }
                ]
            },
        )
        response = route53.list_resource_record_sets(HostedZoneId=hosted_zone)
        txt_records = [
            r
            for r in response["ResourceRecordSets"]
            if r["Type"] == "TXT" and "txt.example.com." in r["Name"]
        ]
        assert len(txt_records) == 1
        # Cleanup
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
                            "ResourceRecords": [
                                {"Value": '"v=spf1 include:example.com ~all"'}
                            ],
                        },
                    }
                ]
            },
        )

    def test_upsert_record(self, route53, hosted_zone):
        """UPSERT creates a record, then updates it in place."""
        # Create via UPSERT
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
                            "ResourceRecords": [{"Value": "10.0.0.1"}],
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
        assert records[0]["ResourceRecords"][0]["Value"] == "10.0.0.1"

        # Update via UPSERT
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
                            "ResourceRecords": [{"Value": "10.0.0.2"}],
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
        assert records[0]["ResourceRecords"][0]["Value"] == "10.0.0.2"

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
                            "ResourceRecords": [{"Value": "10.0.0.2"}],
                        },
                    }
                ]
            },
        )

    def test_multiple_records_in_single_change_batch(self, route53, hosted_zone):
        """Create multiple records in a single ChangeBatch."""
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "multi-a.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "1.1.1.1"}],
                        },
                    },
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "multi-cname.example.com",
                            "Type": "CNAME",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "target.example.com"}],
                        },
                    },
                ]
            },
        )
        response = route53.list_resource_record_sets(HostedZoneId=hosted_zone)
        names = [r["Name"] for r in response["ResourceRecordSets"]]
        assert "multi-a.example.com." in names
        assert "multi-cname.example.com." in names
        # Cleanup
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": "multi-a.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "1.1.1.1"}],
                        },
                    },
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": "multi-cname.example.com",
                            "Type": "CNAME",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "target.example.com"}],
                        },
                    },
                ]
            },
        )

    def test_delete_hosted_zone(self, route53):
        """Create and delete a hosted zone, verify it is gone."""
        ref = _unique("ref")
        response = route53.create_hosted_zone(
            Name="deleteme.example.com",
            CallerReference=ref,
        )
        zone_id = response["HostedZone"]["Id"].split("/")[-1]
        route53.delete_hosted_zone(Id=zone_id)
        response = route53.list_hosted_zones()
        ids = [z["Id"].split("/")[-1] for z in response["HostedZones"]]
        assert zone_id not in ids

    def test_create_hosted_zone_returns_change_info(self, route53):
        """Creating a hosted zone returns ChangeInfo with status."""
        ref = _unique("ref")
        response = route53.create_hosted_zone(
            Name="changeinfo.example.com",
            CallerReference=ref,
        )
        zone_id = response["HostedZone"]["Id"].split("/")[-1]
        assert "ChangeInfo" in response
        assert "Status" in response["ChangeInfo"]
        route53.delete_hosted_zone(Id=zone_id)

    def test_hosted_zone_count(self, route53):
        """get_hosted_zone_count returns a count."""
        response = route53.get_hosted_zone_count()
        assert "HostedZoneCount" in response
        assert isinstance(response["HostedZoneCount"], int)


class TestRoute53Tags:
    def test_change_tags_for_hosted_zone(self, route53):
        """Add and verify tags on a hosted zone."""
        ref = _unique("ref")
        response = route53.create_hosted_zone(
            Name="tagged.example.com",
            CallerReference=ref,
        )
        zone_id = response["HostedZone"]["Id"].split("/")[-1]
        try:
            route53.change_tags_for_resource(
                ResourceType="hostedzone",
                ResourceId=zone_id,
                AddTags=[
                    {"Key": "Environment", "Value": "test"},
                    {"Key": "Team", "Value": "platform"},
                ],
            )
            tag_response = route53.list_tags_for_resource(
                ResourceType="hostedzone",
                ResourceId=zone_id,
            )
            tag_set = tag_response["ResourceTagSet"]
            tag_map = {t["Key"]: t["Value"] for t in tag_set["Tags"]}
            assert tag_map["Environment"] == "test"
            assert tag_map["Team"] == "platform"
        finally:
            route53.delete_hosted_zone(Id=zone_id)

    def test_remove_tags_from_hosted_zone(self, route53):
        """Add tags and then remove one."""
        ref = _unique("ref")
        response = route53.create_hosted_zone(
            Name="untagged.example.com",
            CallerReference=ref,
        )
        zone_id = response["HostedZone"]["Id"].split("/")[-1]
        try:
            route53.change_tags_for_resource(
                ResourceType="hostedzone",
                ResourceId=zone_id,
                AddTags=[
                    {"Key": "Env", "Value": "dev"},
                    {"Key": "Remove", "Value": "me"},
                ],
            )
            route53.change_tags_for_resource(
                ResourceType="hostedzone",
                ResourceId=zone_id,
                RemoveTagKeys=["Remove"],
            )
            tag_response = route53.list_tags_for_resource(
                ResourceType="hostedzone",
                ResourceId=zone_id,
            )
            tag_keys = [t["Key"] for t in tag_response["ResourceTagSet"]["Tags"]]
            assert "Env" in tag_keys
            assert "Remove" not in tag_keys
        finally:
            route53.delete_hosted_zone(Id=zone_id)


class TestRoute53HealthChecks:
    def test_create_health_check(self, route53):
        """Create a health check and verify response."""
        ref = _unique("hc-ref")
        response = route53.create_health_check(
            CallerReference=ref,
            HealthCheckConfig={
                "IPAddress": "10.0.0.1",
                "Port": 80,
                "Type": "HTTP",
                "ResourcePath": "/health",
                "RequestInterval": 30,
                "FailureThreshold": 3,
            },
        )
        hc = response["HealthCheck"]
        hc_id = hc["Id"]
        assert "HealthCheckConfig" in hc
        assert hc["HealthCheckConfig"]["Type"] == "HTTP"
        assert hc["HealthCheckConfig"]["Port"] == 80
        route53.delete_health_check(HealthCheckId=hc_id)

    def test_get_health_check(self, route53):
        """Create and retrieve a health check by ID."""
        ref = _unique("hc-ref")
        create_response = route53.create_health_check(
            CallerReference=ref,
            HealthCheckConfig={
                "IPAddress": "10.0.0.2",
                "Port": 443,
                "Type": "HTTPS",
                "ResourcePath": "/",
                "RequestInterval": 10,
                "FailureThreshold": 2,
            },
        )
        hc_id = create_response["HealthCheck"]["Id"]
        try:
            response = route53.get_health_check(HealthCheckId=hc_id)
            assert response["HealthCheck"]["Id"] == hc_id
            assert response["HealthCheck"]["HealthCheckConfig"]["Type"] == "HTTPS"
        finally:
            route53.delete_health_check(HealthCheckId=hc_id)

    def test_list_health_checks(self, route53):
        """Create a health check and verify it appears in list."""
        ref = _unique("hc-ref")
        create_response = route53.create_health_check(
            CallerReference=ref,
            HealthCheckConfig={
                "IPAddress": "10.0.0.3",
                "Port": 8080,
                "Type": "HTTP",
                "ResourcePath": "/ping",
                "RequestInterval": 30,
                "FailureThreshold": 3,
            },
        )
        hc_id = create_response["HealthCheck"]["Id"]
        try:
            response = route53.list_health_checks()
            assert "HealthChecks" in response
            ids = [hc["Id"] for hc in response["HealthChecks"]]
            assert hc_id in ids
        finally:
            route53.delete_health_check(HealthCheckId=hc_id)

    def test_delete_health_check(self, route53):
        """Create and delete a health check, verify it is gone."""
        ref = _unique("hc-ref")
        create_response = route53.create_health_check(
            CallerReference=ref,
            HealthCheckConfig={
                "IPAddress": "10.0.0.4",
                "Port": 80,
                "Type": "HTTP",
                "ResourcePath": "/",
                "RequestInterval": 30,
                "FailureThreshold": 3,
            },
        )
        hc_id = create_response["HealthCheck"]["Id"]
        route53.delete_health_check(HealthCheckId=hc_id)
        response = route53.list_health_checks()
        ids = [hc["Id"] for hc in response["HealthChecks"]]
        assert hc_id not in ids
