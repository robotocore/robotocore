"""Compatibility tests for AWS Panorama service."""

import json
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
