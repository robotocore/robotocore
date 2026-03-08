"""Compatibility tests for AWS Panorama service."""

import uuid

import pytest

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
        pass


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
                pass

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
                    pass


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
