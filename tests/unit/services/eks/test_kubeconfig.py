"""Tests for kubeconfig generation."""

import base64

import yaml

from robotocore.services.eks.kubeconfig import generate_kubeconfig


class TestGenerateKubeconfig:
    def test_valid_yaml(self):
        result = generate_kubeconfig(
            cluster_name="test-cluster",
            endpoint="http://localhost:12345",
            region="us-east-1",
            account_id="123456789012",
        )
        data = yaml.safe_load(result)
        assert data["apiVersion"] == "v1"
        assert data["kind"] == "Config"

    def test_cluster_name(self):
        result = generate_kubeconfig(
            cluster_name="my-cluster",
            endpoint="http://localhost:9999",
            region="us-west-2",
            account_id="123456789012",
        )
        data = yaml.safe_load(result)
        assert data["clusters"][0]["name"] == "my-cluster"
        assert data["contexts"][0]["context"]["cluster"] == "my-cluster"
        assert data["current-context"] == "my-cluster"

    def test_endpoint_url(self):
        result = generate_kubeconfig(
            cluster_name="test",
            endpoint="http://localhost:54321",
            region="eu-west-1",
            account_id="123456789012",
        )
        data = yaml.safe_load(result)
        assert data["clusters"][0]["cluster"]["server"] == "http://localhost:54321"

    def test_certificate_data_is_base64(self):
        result = generate_kubeconfig(
            cluster_name="test",
            endpoint="http://localhost:1234",
            region="us-east-1",
            account_id="123456789012",
        )
        data = yaml.safe_load(result)
        ca_data = data["clusters"][0]["cluster"]["certificate-authority-data"]
        # Should be valid base64
        decoded = base64.b64decode(ca_data)
        assert b"BEGIN CERTIFICATE" in decoded

    def test_user_token(self):
        result = generate_kubeconfig(
            cluster_name="tok-cluster",
            endpoint="http://localhost:1234",
            region="us-east-1",
            account_id="123456789012",
        )
        data = yaml.safe_load(result)
        assert data["users"][0]["name"] == "tok-cluster-user"
        assert data["users"][0]["user"]["token"] == "k8s-aws-v1.fake-token"
