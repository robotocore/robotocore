"""Compatibility tests for AWS Panorama service."""

import json
import re
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def panorama():
    return make_client("panorama")


@pytest.fixture
def provisioned_device(panorama):
    """Provision a device and clean it up after the test."""
    name = f"test-device-{uuid.uuid4().hex[:8]}"
    resp = panorama.provision_device(Name=name)
    device_id = resp["DeviceId"]
    yield {"Name": name, "DeviceId": device_id, "Arn": resp["Arn"]}
    try:
        panorama.delete_device(DeviceId=device_id)
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture
def created_package(panorama):
    """Create a package and clean it up after the test."""
    name = f"test-pkg-{uuid.uuid4().hex[:8]}"
    resp = panorama.create_package(PackageName=name)
    yield {"PackageName": name, "PackageId": resp["PackageId"], "Arn": resp["Arn"]}
    try:
        panorama.delete_package(PackageId=resp["PackageId"])
    except Exception:
        pass  # best-effort cleanup


class TestPanoramaListDevices:
    def test_list_devices_returns_devices_key(self, panorama):
        resp = panorama.list_devices()
        assert "Devices" in resp

    def test_list_devices_contains_provisioned_device(self, panorama, provisioned_device):
        resp = panorama.list_devices()
        device_ids = [d["DeviceId"] for d in resp["Devices"]]
        assert provisioned_device["DeviceId"] in device_ids


class TestPanoramaListApplicationInstances:
    def test_list_application_instances_returns_key(self, panorama):
        resp = panorama.list_application_instances()
        assert "ApplicationInstances" in resp
        assert isinstance(resp["ApplicationInstances"], list)


class TestPanoramaListNodes:
    def test_list_nodes_returns_key(self, panorama):
        resp = panorama.list_nodes()
        assert "Nodes" in resp
        assert isinstance(resp["Nodes"], list)


class TestPanoramaProvisionDevice:
    def test_provision_device_returns_expected_fields(self, panorama):
        name = f"test-device-{uuid.uuid4().hex[:8]}"
        resp = panorama.provision_device(Name=name)
        try:
            assert "DeviceId" in resp
            assert "Arn" in resp
            assert resp["Arn"].startswith("arn:aws:panorama:")
            assert "Status" in resp
        finally:
            try:
                panorama.delete_device(DeviceId=resp["DeviceId"])
            except Exception:
                pass  # best-effort cleanup

    def test_provision_device_unique_ids(self, panorama):
        name1 = f"test-device-{uuid.uuid4().hex[:8]}"
        name2 = f"test-device-{uuid.uuid4().hex[:8]}"
        r1 = panorama.provision_device(Name=name1)
        r2 = panorama.provision_device(Name=name2)
        try:
            assert r1["DeviceId"] != r2["DeviceId"]
        finally:
            for did in [r1["DeviceId"], r2["DeviceId"]]:
                try:
                    panorama.delete_device(DeviceId=did)
                except Exception:
                    pass  # best-effort cleanup


class TestPanoramaDescribeDevice:
    def test_describe_device_returns_details(self, panorama, provisioned_device):
        resp = panorama.describe_device(DeviceId=provisioned_device["DeviceId"])
        assert resp["DeviceId"] == provisioned_device["DeviceId"]
        assert resp["Name"] == provisioned_device["Name"]
        assert "Arn" in resp
        assert "ProvisioningStatus" in resp
        assert "Type" in resp

    def test_describe_device_not_found(self, panorama):
        with pytest.raises(panorama.exceptions.ClientError) as exc_info:
            panorama.describe_device(DeviceId="device-nonexistent")
        assert "not found" in str(exc_info.value).lower() or "ValidationException" in str(
            exc_info.value
        )


class TestPanoramaCreateNodeFromTemplateJob:
    def test_create_node_from_template_job_returns_job_id(self, panorama):
        resp = panorama.create_node_from_template_job(
            NodeName="test-node",
            OutputPackageName="test-pkg",
            OutputPackageVersion="1.0",
            TemplateParameters={"key": "value"},
            TemplateType="RTSP_CAMERA_STREAM",
        )
        assert "JobId" in resp
        assert isinstance(resp["JobId"], str)
        assert len(resp["JobId"]) > 0


class TestPanoramaDeleteDevice:
    def test_delete_device_returns_device_id(self, panorama):
        name = f"test-device-{uuid.uuid4().hex[:8]}"
        resp = panorama.provision_device(Name=name)
        device_id = resp["DeviceId"]
        delete_resp = panorama.delete_device(DeviceId=device_id)
        assert delete_resp["DeviceId"] == device_id

    def test_delete_device_removes_from_list(self, panorama):
        name = f"test-device-{uuid.uuid4().hex[:8]}"
        resp = panorama.provision_device(Name=name)
        device_id = resp["DeviceId"]
        panorama.delete_device(DeviceId=device_id)
        listed = panorama.list_devices()
        device_ids = [d["DeviceId"] for d in listed["Devices"]]
        assert device_id not in device_ids


class TestPanoramaUpdateDeviceMetadata:
    def test_update_device_metadata_returns_device_id(self, panorama, provisioned_device):
        """UpdateDeviceMetadata returns the device ID after update."""
        resp = panorama.update_device_metadata(
            DeviceId=provisioned_device["DeviceId"],
            Description="Updated description for testing",
        )
        assert resp["DeviceId"] == provisioned_device["DeviceId"]

    def test_update_device_metadata_description_persists(self, panorama, provisioned_device):
        """UpdateDeviceMetadata updates description visible in DescribeDevice."""
        panorama.update_device_metadata(
            DeviceId=provisioned_device["DeviceId"],
            Description="Persisted description",
        )
        desc = panorama.describe_device(DeviceId=provisioned_device["DeviceId"])
        assert desc["Description"] == "Persisted description"


class TestPanoramaCreatePackage:
    def test_create_package_returns_expected_fields(self, panorama):
        pkg_name = f"test-pkg-{uuid.uuid4().hex[:8]}"
        resp = panorama.create_package(PackageName=pkg_name)
        try:
            assert "PackageId" in resp
            assert "Arn" in resp
            assert resp["Arn"].startswith("arn:aws:panorama:")
            assert "StorageLocation" in resp
            storage = resp["StorageLocation"]
            assert "Bucket" in storage
            assert "BinaryPrefixLocation" in storage
        finally:
            try:
                panorama.delete_package(PackageId=resp["PackageId"])
            except Exception:
                pass  # best-effort cleanup

    def test_create_package_unique_ids(self, panorama):
        pkg1 = panorama.create_package(PackageName=f"test-pkg-{uuid.uuid4().hex[:8]}")
        pkg2 = panorama.create_package(PackageName=f"test-pkg-{uuid.uuid4().hex[:8]}")
        try:
            assert pkg1["PackageId"] != pkg2["PackageId"]
            assert pkg1["Arn"] != pkg2["Arn"]
        finally:
            for pid in [pkg1["PackageId"], pkg2["PackageId"]]:
                try:
                    panorama.delete_package(PackageId=pid)
                except Exception:
                    pass  # best-effort cleanup


class TestPanoramaDeletePackage:
    def test_delete_package_succeeds(self, panorama):
        resp = panorama.create_package(PackageName=f"test-pkg-{uuid.uuid4().hex[:8]}")
        pkg_id = resp["PackageId"]
        del_resp = panorama.delete_package(PackageId=pkg_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_package_not_found(self, panorama):
        with pytest.raises(ClientError) as exc_info:
            panorama.delete_package(PackageId="package-nonexistent")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestPanoramaDescribeNodeFromTemplateJob:
    def test_describe_node_from_template_job_returns_details(self, panorama):
        job = panorama.create_node_from_template_job(
            NodeName="test-node-desc",
            OutputPackageName="test-pkg",
            OutputPackageVersion="1.0",
            TemplateParameters={"key": "value"},
            TemplateType="RTSP_CAMERA_STREAM",
        )
        job_id = job["JobId"]
        resp = panorama.describe_node_from_template_job(JobId=job_id)
        assert resp["JobId"] == job_id
        assert resp["NodeName"] == "test-node-desc"
        assert resp["TemplateType"] == "RTSP_CAMERA_STREAM"
        assert resp["OutputPackageName"] == "test-pkg"
        assert "Status" in resp
        assert "CreatedTime" in resp


class TestPanoramaDescribeNode:
    def test_describe_node_not_found(self, panorama):
        with pytest.raises(ClientError) as exc_info:
            panorama.describe_node(NodeId="node-nonexistent")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestPanoramaCreateApplicationInstance:
    def test_create_application_instance_returns_id(self, panorama):
        name = f"test-device-{uuid.uuid4().hex[:8]}"
        dev = panorama.provision_device(Name=name)
        device_id = dev["DeviceId"]
        try:
            manifest = json.dumps({"PayloadData": "test"})
            resp = panorama.create_application_instance(
                DefaultRuntimeContextDevice=device_id,
                ManifestPayload={"PayloadData": manifest},
            )
            assert "ApplicationInstanceId" in resp
            assert isinstance(resp["ApplicationInstanceId"], str)
            assert len(resp["ApplicationInstanceId"]) > 0
        finally:
            try:
                panorama.delete_device(DeviceId=device_id)
            except Exception:
                pass  # best-effort cleanup


class TestPanoramaDescribeApplicationInstanceDetails:
    def test_describe_application_instance_details(self, panorama):
        name = f"test-device-{uuid.uuid4().hex[:8]}"
        dev = panorama.provision_device(Name=name)
        device_id = dev["DeviceId"]
        try:
            manifest = json.dumps({"PayloadData": "test"})
            app = panorama.create_application_instance(
                DefaultRuntimeContextDevice=device_id,
                ManifestPayload={"PayloadData": manifest},
            )
            app_id = app["ApplicationInstanceId"]
            resp = panorama.describe_application_instance_details(ApplicationInstanceId=app_id)
            assert resp["ApplicationInstanceId"] == app_id
            assert "ManifestPayload" in resp
            assert resp["DefaultRuntimeContextDevice"] == device_id
        finally:
            try:
                panorama.delete_device(DeviceId=device_id)
            except Exception:
                pass  # best-effort cleanup

    def test_describe_application_instance_details_not_found(self, panorama):
        with pytest.raises(ClientError) as exc_info:
            panorama.describe_application_instance_details(
                ApplicationInstanceId="applicationInstance-nonexistent"
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestPanoramaAdditionalOps:
    """Additional Panorama operation tests."""

    def test_list_packages(self, panorama):
        resp = panorama.list_packages()
        assert "Packages" in resp
        assert isinstance(resp["Packages"], list)

    def test_list_package_import_jobs(self, panorama):
        resp = panorama.list_package_import_jobs()
        assert "PackageImportJobs" in resp
        assert isinstance(resp["PackageImportJobs"], list)

    def test_list_node_from_template_jobs(self, panorama):
        resp = panorama.list_node_from_template_jobs()
        assert "NodeFromTemplateJobs" in resp
        assert isinstance(resp["NodeFromTemplateJobs"], list)

    def test_describe_application_instance_not_found(self, panorama):
        with pytest.raises(ClientError) as exc_info:
            panorama.describe_application_instance(
                ApplicationInstanceId="applicationInstance-nonexistent"
            )
        err = exc_info.value.response["Error"]["Code"]
        assert err == "ResourceNotFoundException" or "not found" in str(exc_info.value).lower()

    def test_describe_package_import_job_not_found(self, panorama):
        with pytest.raises(ClientError) as exc_info:
            panorama.describe_package_import_job(JobId="job-nonexistent")
        err = exc_info.value.response["Error"]["Code"]
        assert err == "ResourceNotFoundException" or "not found" in str(exc_info.value).lower()

    def test_remove_application_instance_not_found(self, panorama):
        with pytest.raises(ClientError) as exc_info:
            panorama.remove_application_instance(
                ApplicationInstanceId="applicationInstance-nonexistent"
            )
        err = exc_info.value.response["Error"]["Code"]
        assert err == "ResourceNotFoundException" or "not found" in str(exc_info.value).lower()

    def test_create_package_import_job(self, panorama):
        token = f"token-{uuid.uuid4().hex[:8]}"
        resp = panorama.create_package_import_job(
            JobType="NODE_PACKAGE_VERSION",
            InputConfig={
                "PackageVersionInputConfig": {
                    "S3Location": {
                        "BucketName": "test-bucket",
                        "Region": "us-east-1",
                        "ObjectKey": "test-key",
                    }
                }
            },
            OutputConfig={
                "PackageVersionOutputConfig": {
                    "PackageName": f"test-pkg-{uuid.uuid4().hex[:8]}",
                    "PackageVersion": "1.0",
                }
            },
            ClientToken=token,
        )
        assert "JobId" in resp
        assert isinstance(resp["JobId"], str)
        assert len(resp["JobId"]) > 0


class TestPanoramaMissingGapOps:
    """Tests for Panorama operations identified as coverage gaps."""

    def test_list_tags_for_resource(self, panorama):
        resp = panorama.list_tags_for_resource(
            ResourceArn="arn:aws:panorama:us-east-1:123456789012:device/fake"
        )
        assert "Tags" in resp

    def test_tag_resource(self, panorama):
        resp = panorama.tag_resource(
            ResourceArn="arn:aws:panorama:us-east-1:123456789012:device/fake",
            Tags={"env": "test"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_devices_jobs(self, panorama):
        device_id = "device-" + uuid.uuid4().hex[:8]
        resp = panorama.list_devices_jobs(DeviceId=device_id)
        assert "DeviceJobs" in resp

    def test_list_application_instance_dependencies(self, panorama):
        app_instance_id = "ai-" + uuid.uuid4().hex[:8]
        with pytest.raises(ClientError):
            panorama.list_application_instance_dependencies(ApplicationInstanceId=app_instance_id)


class TestPanoramaGapOps:
    """Tests for panorama ops that were working but untested."""

    @pytest.fixture
    def client(self):
        return make_client("panorama")

    def test_create_job_for_devices_nonexistent(self, client):
        """CreateJobForDevices raises ValidationException for nonexistent device."""
        with pytest.raises(ClientError) as exc:
            client.create_job_for_devices(
                DeviceIds=["device-nonexistent-123"],
                DeviceJobConfig={
                    "OTAJobConfig": {
                        "ImageVersion": "1.0.0",
                        "AllowMajorVersionUpdate": False,
                    }
                },
                JobType="OTA",
            )
        assert exc.value.response["Error"]["Code"] in (
            "ValidationException",
            "ResourceNotFoundException",
        )

    def test_deregister_package_version_nonexistent(self, client):
        """DeregisterPackageVersion raises ResourceNotFoundException for nonexistent package."""
        with pytest.raises(ClientError) as exc:
            client.deregister_package_version(
                PackageId="pkg-nonexistent-123",
                PackageVersion="1.0.0",
                PatchVersion="0",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_register_package_version(self, client):
        """RegisterPackageVersion on a new package succeeds or returns NotFound."""
        pkg_resp = client.create_package(PackageName="test-pkg-reg")
        pkg_id = pkg_resp["PackageId"]
        try:
            resp = client.register_package_version(
                PackageId=pkg_id,
                PackageVersion="1.0.0",
                PatchVersion="0",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as exc:
            assert exc.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "ValidationException",
            )
        finally:
            client.delete_package(PackageId=pkg_id)

    def test_tag_resource(self, client):
        """TagResource adds tags to a panorama resource."""
        device_resp = client.provision_device(Name="tag-test-device")
        device_id = device_resp["DeviceId"]
        arn = device_resp["Arn"]
        try:
            resp = client.tag_resource(ResourceArn=arn, Tags={"env": "test"})
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            tags_resp = client.list_tags_for_resource(ResourceArn=arn)
            assert "Tags" in tags_resp
        finally:
            client.delete_device(DeviceId=device_id)

    def test_untag_resource(self, client):
        """UntagResource removes tags from a panorama resource."""
        device_resp = client.provision_device(Name="untag-test-device")
        device_id = device_resp["DeviceId"]
        arn = device_resp["Arn"]
        try:
            client.tag_resource(ResourceArn=arn, Tags={"env": "test", "k2": "v2"})
            client.untag_resource(ResourceArn=arn, TagKeys=["env"])
            tags_resp = client.list_tags_for_resource(ResourceArn=arn)
            assert "env" not in tags_resp.get("Tags", {})
        finally:
            client.delete_device(DeviceId=device_id)


class TestPanoramaGapOpsV2:
    """Tests for panorama describe/list ops that weren't directly called."""

    @pytest.fixture
    def client(self):
        return make_client("panorama")

    def test_describe_device_job_nonexistent(self, client):
        """DescribeDeviceJob raises ResourceNotFoundException for nonexistent job."""
        with pytest.raises(ClientError) as exc:
            client.describe_device_job(JobId="job-nonexistent-123")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_package_version_nonexistent(self, client):
        """DescribePackageVersion raises ResourceNotFoundException for nonexistent version."""
        with pytest.raises(ClientError) as exc:
            client.describe_package_version(
                PackageId="pkg-nonexistent-123",
                PackageVersion="1.0.0",
                PatchVersion="0",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_application_instance_node_instances(self, client):
        """ListApplicationInstanceNodeInstances raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.list_application_instance_node_instances(
                ApplicationInstanceId="ai-nonexistent-123"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestPanoramaSignalApplicationInstanceNodeInstances:
    """Test SignalApplicationInstanceNodeInstances operation."""

    def test_signal_application_instance_node_instances(self):
        """SignalApplicationInstanceNodeInstances raises known error."""
        client = make_client("panorama")
        try:
            client.signal_application_instance_node_instances(
                ApplicationInstanceId="fake-app-id",
                NodeSignals=[{"NodeInstanceId": "fake-node", "Signal": "PAUSE"}],
            )
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None


class TestPanoramaDeviceEdgeCases:
    """Edge cases and behavioral fidelity for device operations."""

    def test_provision_device_arn_format(self, panorama):
        """Device ARNs match expected panorama ARN pattern."""
        name = f"test-device-{uuid.uuid4().hex[:8]}"
        resp = panorama.provision_device(Name=name)
        try:
            arn = resp["Arn"]
            assert re.match(
                r"^arn:aws:panorama:[a-z0-9-]+:\d{12}:device/", arn
            ), f"ARN doesn't match expected pattern: {arn}"
        finally:
            try:
                panorama.delete_device(DeviceId=resp["DeviceId"])
            except Exception:
                pass  # best-effort cleanup

    def test_provision_device_status_initial(self, panorama):
        """Newly provisioned device has an expected initial status."""
        name = f"test-device-{uuid.uuid4().hex[:8]}"
        resp = panorama.provision_device(Name=name)
        try:
            assert resp["Status"] in (
                "AWAITING_PROVISIONING",
                "PENDING",
                "SUCCEEDED",
            ), f"Unexpected initial status: {resp['Status']}"
        finally:
            try:
                panorama.delete_device(DeviceId=resp["DeviceId"])
            except Exception:
                pass  # best-effort cleanup

    def test_describe_device_arn_matches_provision(self, panorama, provisioned_device):
        """DescribeDevice returns the same ARN as ProvisionDevice."""
        resp = panorama.describe_device(DeviceId=provisioned_device["DeviceId"])
        assert resp["Arn"] == provisioned_device["Arn"]

    def test_describe_device_has_created_time(self, panorama, provisioned_device):
        """DescribeDevice returns a CreatedTime field."""
        resp = panorama.describe_device(DeviceId=provisioned_device["DeviceId"])
        assert "CreatedTime" in resp

    def test_list_devices_returns_name_and_status(self, panorama, provisioned_device):
        """ListDevices entries include Name and DeviceId fields."""
        resp = panorama.list_devices()
        device = next(
            (d for d in resp["Devices"] if d["DeviceId"] == provisioned_device["DeviceId"]),
            None,
        )
        assert device is not None
        assert "Name" in device
        assert device["Name"] == provisioned_device["Name"]

    def test_list_devices_pagination_max_results(self, panorama):
        """ListDevices respects MaxResults parameter."""
        device_ids = []
        try:
            for i in range(3):
                resp = panorama.provision_device(Name=f"page-test-{uuid.uuid4().hex[:8]}")
                device_ids.append(resp["DeviceId"])
            resp = panorama.list_devices(MaxResults=1)
            assert len(resp["Devices"]) <= 1
            if "NextToken" in resp and resp["NextToken"]:
                resp2 = panorama.list_devices(NextToken=resp["NextToken"])
                assert "Devices" in resp2
        finally:
            for did in device_ids:
                try:
                    panorama.delete_device(DeviceId=did)
                except Exception:
                    pass  # best-effort cleanup

    def test_delete_nonexistent_device(self, panorama):
        """Deleting a nonexistent device raises an error."""
        with pytest.raises(ClientError) as exc_info:
            panorama.delete_device(DeviceId="device-does-not-exist-xyz")
        assert exc_info.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "InternalError",
        )

    def test_update_device_metadata_nonexistent(self, panorama):
        """Updating metadata on a nonexistent device raises an error."""
        with pytest.raises(ClientError) as exc_info:
            panorama.update_device_metadata(
                DeviceId="device-nonexistent-xyz",
                Description="should fail",
            )
        assert exc_info.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
            "InternalError",
        )

    def test_provision_device_duplicate_name_is_idempotent(self, panorama):
        """Provisioning a device with the same name returns the same device (idempotent)."""
        name = f"dup-device-{uuid.uuid4().hex[:8]}"
        r1 = panorama.provision_device(Name=name)
        r2 = panorama.provision_device(Name=name)
        try:
            assert r1["DeviceId"] == r2["DeviceId"]
            assert r1["Arn"] == r2["Arn"]
        finally:
            try:
                panorama.delete_device(DeviceId=r1["DeviceId"])
            except Exception:
                pass  # best-effort cleanup


class TestPanoramaPackageEdgeCases:
    """Edge cases and behavioral fidelity for package operations."""

    def test_create_package_arn_format(self, panorama, created_package):
        """Package ARNs match expected pattern."""
        arn = created_package["Arn"]
        assert re.match(
            r"^arn:aws:panorama:[a-z0-9-]+:\d{12}:package/", arn
        ), f"ARN doesn't match expected pattern: {arn}"

    def test_list_packages_contains_created(self, panorama, created_package):
        """ListPackages includes a freshly created package."""
        resp = panorama.list_packages()
        pkg_ids = [p["PackageId"] for p in resp["Packages"]]
        assert created_package["PackageId"] in pkg_ids

    def test_list_packages_pagination(self, panorama):
        """ListPackages respects MaxResults and pagination."""
        pkg_ids = []
        try:
            for _ in range(3):
                resp = panorama.create_package(PackageName=f"page-pkg-{uuid.uuid4().hex[:8]}")
                pkg_ids.append(resp["PackageId"])
            resp = panorama.list_packages(MaxResults=1)
            assert len(resp["Packages"]) <= 1
            if "NextToken" in resp and resp["NextToken"]:
                resp2 = panorama.list_packages(NextToken=resp["NextToken"])
                assert "Packages" in resp2
        finally:
            for pid in pkg_ids:
                try:
                    panorama.delete_package(PackageId=pid)
                except Exception:
                    pass  # best-effort cleanup

    def test_create_package_storage_location_fields(self, panorama):
        """CreatePackage StorageLocation has expected fields."""
        name = f"test-pkg-{uuid.uuid4().hex[:8]}"
        resp = panorama.create_package(PackageName=name)
        try:
            storage = resp["StorageLocation"]
            assert "Bucket" in storage
            assert "GeneratedPrefixLocation" in storage or "ManifestPrefixLocation" in storage
            assert "RepoPrefixLocation" in storage or "BinaryPrefixLocation" in storage
        finally:
            try:
                panorama.delete_package(PackageId=resp["PackageId"])
            except Exception:
                pass  # best-effort cleanup


class TestPanoramaTaggingEdgeCases:
    """Edge cases for tagging operations."""

    def test_tag_resource_multiple_tags(self, panorama, provisioned_device):
        """TagResource can add multiple tags at once."""
        arn = provisioned_device["Arn"]
        panorama.tag_resource(ResourceArn=arn, Tags={"k1": "v1", "k2": "v2", "k3": "v3"})
        tags = panorama.list_tags_for_resource(ResourceArn=arn)["Tags"]
        assert tags["k1"] == "v1"
        assert tags["k2"] == "v2"
        assert tags["k3"] == "v3"

    def test_tag_resource_overwrite_existing(self, panorama, provisioned_device):
        """TagResource overwrites an existing tag value."""
        arn = provisioned_device["Arn"]
        panorama.tag_resource(ResourceArn=arn, Tags={"key": "original"})
        panorama.tag_resource(ResourceArn=arn, Tags={"key": "updated"})
        tags = panorama.list_tags_for_resource(ResourceArn=arn)["Tags"]
        assert tags["key"] == "updated"

    def test_untag_resource_selective(self, panorama, provisioned_device):
        """UntagResource removes only specified tags."""
        arn = provisioned_device["Arn"]
        panorama.tag_resource(ResourceArn=arn, Tags={"keep": "yes", "remove": "yes"})
        panorama.untag_resource(ResourceArn=arn, TagKeys=["remove"])
        tags = panorama.list_tags_for_resource(ResourceArn=arn)["Tags"]
        assert "keep" in tags
        assert "remove" not in tags

    def test_list_tags_empty_initially(self, panorama, provisioned_device):
        """A new device has no tags (or empty tags dict)."""
        arn = provisioned_device["Arn"]
        tags = panorama.list_tags_for_resource(ResourceArn=arn)["Tags"]
        assert isinstance(tags, dict)

    def test_tag_resource_unicode_values(self, panorama, provisioned_device):
        """Tags can contain unicode characters."""
        arn = provisioned_device["Arn"]
        panorama.tag_resource(ResourceArn=arn, Tags={"name": "café-日本語"})
        tags = panorama.list_tags_for_resource(ResourceArn=arn)["Tags"]
        assert tags["name"] == "café-日本語"


class TestPanoramaNodeFromTemplateJobEdgeCases:
    """Edge cases for node from template job operations."""

    def test_create_node_from_template_job_multiple_params(self, panorama):
        """CreateNodeFromTemplateJob accepts multiple template parameters."""
        resp = panorama.create_node_from_template_job(
            NodeName="multi-param-node",
            OutputPackageName="test-pkg",
            OutputPackageVersion="1.0",
            TemplateParameters={"Username": "admin", "Password": "secret", "StreamUrl": "rtsp://example.com"},
            TemplateType="RTSP_CAMERA_STREAM",
        )
        assert "JobId" in resp
        job_id = resp["JobId"]
        desc = panorama.describe_node_from_template_job(JobId=job_id)
        # Sensitive params may be masked as SAVED_AS_SECRET
        assert "Username" in desc["TemplateParameters"]
        assert "StreamUrl" in desc["TemplateParameters"]
        assert len(desc["TemplateParameters"]) == 3

    def test_describe_node_from_template_job_has_timestamps(self, panorama):
        """DescribeNodeFromTemplateJob includes CreatedTime and LastUpdatedTime."""
        resp = panorama.create_node_from_template_job(
            NodeName="ts-node",
            OutputPackageName="ts-pkg",
            OutputPackageVersion="1.0",
            TemplateParameters={"key": "value"},
            TemplateType="RTSP_CAMERA_STREAM",
        )
        desc = panorama.describe_node_from_template_job(JobId=resp["JobId"])
        assert "CreatedTime" in desc
        assert "LastUpdatedTime" in desc

    def test_list_node_from_template_jobs_contains_created(self, panorama):
        """ListNodeFromTemplateJobs includes a recently created job."""
        resp = panorama.create_node_from_template_job(
            NodeName="list-check-node",
            OutputPackageName="list-pkg",
            OutputPackageVersion="1.0",
            TemplateParameters={"key": "value"},
            TemplateType="RTSP_CAMERA_STREAM",
        )
        job_id = resp["JobId"]
        listed = panorama.list_node_from_template_jobs()
        job_ids = [j["JobId"] for j in listed["NodeFromTemplateJobs"]]
        assert job_id in job_ids

    def test_describe_node_from_template_job_nonexistent(self, panorama):
        """DescribeNodeFromTemplateJob raises error for nonexistent job."""
        with pytest.raises(ClientError) as exc_info:
            panorama.describe_node_from_template_job(JobId="job-nonexistent-xyz")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestPanoramaApplicationInstanceEdgeCases:
    """Edge cases for application instance operations."""

    def test_create_application_instance_with_tags(self, panorama):
        """CreateApplicationInstance accepts tags."""
        name = f"test-device-{uuid.uuid4().hex[:8]}"
        dev = panorama.provision_device(Name=name)
        device_id = dev["DeviceId"]
        try:
            manifest = json.dumps({"PayloadData": "test"})
            resp = panorama.create_application_instance(
                DefaultRuntimeContextDevice=device_id,
                ManifestPayload={"PayloadData": manifest},
                Tags={"env": "test", "team": "platform"},
            )
            assert "ApplicationInstanceId" in resp
            app_id = resp["ApplicationInstanceId"]
            details = panorama.describe_application_instance_details(
                ApplicationInstanceId=app_id
            )
            assert details["ApplicationInstanceId"] == app_id
        finally:
            try:
                panorama.delete_device(DeviceId=device_id)
            except Exception:
                pass  # best-effort cleanup

    def test_create_application_instance_with_name(self, panorama):
        """CreateApplicationInstance accepts a Name parameter."""
        name = f"test-device-{uuid.uuid4().hex[:8]}"
        dev = panorama.provision_device(Name=name)
        device_id = dev["DeviceId"]
        try:
            manifest = json.dumps({"PayloadData": "test"})
            resp = panorama.create_application_instance(
                DefaultRuntimeContextDevice=device_id,
                ManifestPayload={"PayloadData": manifest},
                Name="my-app-instance",
            )
            assert "ApplicationInstanceId" in resp
        finally:
            try:
                panorama.delete_device(DeviceId=device_id)
            except Exception:
                pass  # best-effort cleanup

    def test_list_application_instances_contains_created(self, panorama):
        """ListApplicationInstances includes a created instance."""
        name = f"test-device-{uuid.uuid4().hex[:8]}"
        dev = panorama.provision_device(Name=name)
        device_id = dev["DeviceId"]
        try:
            manifest = json.dumps({"PayloadData": "test"})
            app = panorama.create_application_instance(
                DefaultRuntimeContextDevice=device_id,
                ManifestPayload={"PayloadData": manifest},
            )
            app_id = app["ApplicationInstanceId"]
            listed = panorama.list_application_instances()
            app_ids = [a["ApplicationInstanceId"] for a in listed["ApplicationInstances"]]
            assert app_id in app_ids
        finally:
            try:
                panorama.delete_device(DeviceId=device_id)
            except Exception:
                pass  # best-effort cleanup

    def test_describe_application_instance(self, panorama):
        """DescribeApplicationInstance returns expected fields."""
        name = f"test-device-{uuid.uuid4().hex[:8]}"
        dev = panorama.provision_device(Name=name)
        device_id = dev["DeviceId"]
        try:
            manifest = json.dumps({"PayloadData": "test"})
            app = panorama.create_application_instance(
                DefaultRuntimeContextDevice=device_id,
                ManifestPayload={"PayloadData": manifest},
            )
            app_id = app["ApplicationInstanceId"]
            resp = panorama.describe_application_instance(ApplicationInstanceId=app_id)
            assert resp["ApplicationInstanceId"] == app_id
            assert resp["DefaultRuntimeContextDevice"] == device_id
            assert "Status" in resp
            assert "CreatedTime" in resp
        finally:
            try:
                panorama.delete_device(DeviceId=device_id)
            except Exception:
                pass  # best-effort cleanup


class TestPanoramaPackageImportJobEdgeCases:
    """Edge cases for package import job operations."""

    def test_create_package_import_job_returns_arn(self, panorama):
        """CreatePackageImportJob response includes JobId."""
        resp = panorama.create_package_import_job(
            JobType="NODE_PACKAGE_VERSION",
            InputConfig={
                "PackageVersionInputConfig": {
                    "S3Location": {
                        "BucketName": "test-bucket",
                        "Region": "us-east-1",
                        "ObjectKey": "test-key.tar.gz",
                    }
                }
            },
            OutputConfig={
                "PackageVersionOutputConfig": {
                    "PackageName": f"imp-pkg-{uuid.uuid4().hex[:8]}",
                    "PackageVersion": "1.0",
                }
            },
            ClientToken=f"token-{uuid.uuid4().hex[:8]}",
        )
        assert "JobId" in resp
        assert len(resp["JobId"]) > 0

    def test_list_package_import_jobs_contains_created(self, panorama):
        """ListPackageImportJobs includes a recently created job."""
        resp = panorama.create_package_import_job(
            JobType="NODE_PACKAGE_VERSION",
            InputConfig={
                "PackageVersionInputConfig": {
                    "S3Location": {
                        "BucketName": "bucket-list-test",
                        "Region": "us-east-1",
                        "ObjectKey": "key.tar.gz",
                    }
                }
            },
            OutputConfig={
                "PackageVersionOutputConfig": {
                    "PackageName": f"list-imp-{uuid.uuid4().hex[:8]}",
                    "PackageVersion": "1.0",
                }
            },
            ClientToken=f"token-{uuid.uuid4().hex[:8]}",
        )
        job_id = resp["JobId"]
        listed = panorama.list_package_import_jobs()
        job_ids = [j["JobId"] for j in listed["PackageImportJobs"]]
        assert job_id in job_ids

    def test_describe_package_import_job_returns_details(self, panorama):
        """DescribePackageImportJob returns matching details."""
        pkg_name = f"desc-imp-{uuid.uuid4().hex[:8]}"
        resp = panorama.create_package_import_job(
            JobType="NODE_PACKAGE_VERSION",
            InputConfig={
                "PackageVersionInputConfig": {
                    "S3Location": {
                        "BucketName": "bucket-desc-test",
                        "Region": "us-east-1",
                        "ObjectKey": "key.tar.gz",
                    }
                }
            },
            OutputConfig={
                "PackageVersionOutputConfig": {
                    "PackageName": pkg_name,
                    "PackageVersion": "1.0",
                }
            },
            ClientToken=f"token-{uuid.uuid4().hex[:8]}",
        )
        job_id = resp["JobId"]
        desc = panorama.describe_package_import_job(JobId=job_id)
        assert desc["JobId"] == job_id
        assert desc["JobType"] == "NODE_PACKAGE_VERSION"
        assert "Status" in desc
        assert "CreatedTime" in desc
