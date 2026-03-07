"""Route53 compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

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


class TestRoute53Tags:
    def test_create_hosted_zone_and_tag(self, route53):
        ref = f"ref-tag-{uuid.uuid4().hex[:8]}"
        zone = route53.create_hosted_zone(Name="tagged.example.com", CallerReference=ref)
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            route53.change_tags_for_resource(
                ResourceType="hostedzone",
                ResourceId=zone_id,
                AddTags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "platform"},
                ],
            )
            resp = route53.list_tags_for_resource(
                ResourceType="hostedzone", ResourceId=zone_id
            )
            tags = {t["Key"]: t["Value"] for t in resp["ResourceTagSet"]["Tags"]}
            assert tags["env"] == "test"
            assert tags["team"] == "platform"
        finally:
            route53.delete_hosted_zone(Id=zone_id)

    def test_untag_hosted_zone(self, route53):
        ref = f"ref-untag-{uuid.uuid4().hex[:8]}"
        zone = route53.create_hosted_zone(Name="untag.example.com", CallerReference=ref)
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            route53.change_tags_for_resource(
                ResourceType="hostedzone",
                ResourceId=zone_id,
                AddTags=[{"Key": "remove-me", "Value": "yes"}],
            )
            route53.change_tags_for_resource(
                ResourceType="hostedzone",
                ResourceId=zone_id,
                RemoveTagKeys=["remove-me"],
            )
            resp = route53.list_tags_for_resource(
                ResourceType="hostedzone", ResourceId=zone_id
            )
            tag_keys = [t["Key"] for t in resp["ResourceTagSet"]["Tags"]]
            assert "remove-me" not in tag_keys
        finally:
            route53.delete_hosted_zone(Id=zone_id)


class TestRoute53RecordTypes:
    def test_mx_record(self, route53, hosted_zone):
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

    def test_txt_record(self, route53, hosted_zone):
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
                            "ResourceRecords": [
                                {"Value": '"v=spf1 include:example.com ~all"'}
                            ],
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
                            "ResourceRecords": [
                                {"Value": '"v=spf1 include:example.com ~all"'}
                            ],
                        },
                    }
                ]
            },
        )


class TestRoute53Pagination:
    def test_list_record_sets_with_max_items(self, route53, hosted_zone):
        """List record sets with MaxItems to test pagination."""
        # Default zone has SOA + NS = 2 records
        response = route53.list_resource_record_sets(
            HostedZoneId=hosted_zone, MaxItems="1"
        )
        assert len(response["ResourceRecordSets"]) == 1
        assert response["IsTruncated"] is True


class TestRoute53HostedZoneCount:
    def test_get_hosted_zone_count(self, route53):
        ref = f"ref-count-{uuid.uuid4().hex[:8]}"
        zone = route53.create_hosted_zone(Name="count.example.com", CallerReference=ref)
        zone_id = zone["HostedZone"]["Id"].split("/")[-1]
        try:
            response = route53.get_hosted_zone_count()
            assert response["HostedZoneCount"] >= 1
        finally:
            route53.delete_hosted_zone(Id=zone_id)


class TestRoute53HealthCheck:
    def test_create_and_delete_health_check(self, route53):
        ref = f"hc-ref-{uuid.uuid4().hex[:8]}"
        resp = route53.create_health_check(
            CallerReference=ref,
            HealthCheckConfig={
                "Type": "HTTP",
                "FullyQualifiedDomainName": "example.com",
                "Port": 80,
                "RequestInterval": 30,
                "FailureThreshold": 3,
            },
        )
        hc_id = resp["HealthCheck"]["Id"]
        try:
            listed = route53.list_health_checks()
            hc_ids = [hc["Id"] for hc in listed["HealthChecks"]]
            assert hc_id in hc_ids
        finally:
            route53.delete_health_check(HealthCheckId=hc_id)

    def test_health_check_not_in_list_after_delete(self, route53):
        ref = f"hc-del-{uuid.uuid4().hex[:8]}"
        resp = route53.create_health_check(
            CallerReference=ref,
            HealthCheckConfig={
                "Type": "TCP",
                "IPAddress": "1.2.3.4",
                "Port": 443,
                "RequestInterval": 30,
                "FailureThreshold": 3,
            },
        )
        hc_id = resp["HealthCheck"]["Id"]
        route53.delete_health_check(HealthCheckId=hc_id)
        listed = route53.list_health_checks()
        hc_ids = [hc["Id"] for hc in listed["HealthChecks"]]
        assert hc_id not in hc_ids


class TestRoute53UpsertRecord:
    def test_upsert_creates_and_updates(self, route53, hosted_zone):
        """UPSERT creates a record then updates it."""
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
        # UPSERT to update value
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
                            "ResourceRecords": [{"Value": "2.2.2.2"}],
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
        assert records[0]["ResourceRecords"][0]["Value"] == "2.2.2.2"
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
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "2.2.2.2"}],
                        },
                    }
                ]
            },
        )


class TestRoute53MultipleRecordChanges:
    def test_batch_create_multiple_records(self, route53, hosted_zone):
        """Create multiple records in a single ChangeBatch."""
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "svc1.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "10.0.1.1"}],
                        },
                    },
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "svc2.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "10.0.2.1"}],
                        },
                    },
                ]
            },
        )
        response = route53.list_resource_record_sets(HostedZoneId=hosted_zone)
        names = [r["Name"] for r in response["ResourceRecordSets"]]
        assert "svc1.example.com." in names
        assert "svc2.example.com." in names
        # Cleanup
        route53.change_resource_record_sets(
            HostedZoneId=hosted_zone,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": "svc1.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "10.0.1.1"}],
                        },
                    },
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": "svc2.example.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "10.0.2.1"}],
                        },
                    },
                ]
            },
        )
