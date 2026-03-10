"""Tests for kubeconfig generation."""

import base64

import yaml

from robotocore.services.eks.kubeconfig import _FAKE_CA_CERT, generate_kubeconfig


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

    def test_all_required_keys_present(self):
        result = generate_kubeconfig(
            cluster_name="full",
            endpoint="http://localhost:5555",
            region="us-east-1",
            account_id="123456789012",
        )
        data = yaml.safe_load(result)
        assert "apiVersion" in data
        assert "kind" in data
        assert "clusters" in data
        assert "contexts" in data
        assert "users" in data
        assert "current-context" in data

    def test_context_references_user(self):
        result = generate_kubeconfig(
            cluster_name="ctx-test",
            endpoint="http://localhost:1234",
            region="us-east-1",
            account_id="123456789012",
        )
        data = yaml.safe_load(result)
        ctx_user = data["contexts"][0]["context"]["user"]
        user_names = [u["name"] for u in data["users"]]
        assert ctx_user in user_names

    def test_different_clusters_produce_different_configs(self):
        cfg1 = yaml.safe_load(
            generate_kubeconfig("cluster-a", "http://localhost:1111", "us-east-1", "111111111111")
        )
        cfg2 = yaml.safe_load(
            generate_kubeconfig("cluster-b", "http://localhost:2222", "us-west-2", "222222222222")
        )
        assert cfg1["clusters"][0]["name"] != cfg2["clusters"][0]["name"]
        assert cfg1["clusters"][0]["cluster"]["server"] != cfg2["clusters"][0]["cluster"]["server"]
        assert cfg1["current-context"] != cfg2["current-context"]

    def test_exactly_one_cluster_context_user(self):
        result = generate_kubeconfig(
            cluster_name="single",
            endpoint="http://localhost:1234",
            region="us-east-1",
            account_id="123456789012",
        )
        data = yaml.safe_load(result)
        assert len(data["clusters"]) == 1
        assert len(data["contexts"]) == 1
        assert len(data["users"]) == 1

    def test_certificate_matches_fake_ca_cert(self):
        result = generate_kubeconfig(
            cluster_name="cert-test",
            endpoint="http://localhost:1234",
            region="us-east-1",
            account_id="123456789012",
        )
        data = yaml.safe_load(result)
        ca_data = data["clusters"][0]["cluster"]["certificate-authority-data"]
        decoded = base64.b64decode(ca_data)
        assert decoded == _FAKE_CA_CERT

    def test_cluster_name_with_special_characters(self):
        """Cluster names with hyphens and numbers should work fine."""
        result = generate_kubeconfig(
            cluster_name="my-cluster-123",
            endpoint="http://localhost:1234",
            region="us-east-1",
            account_id="123456789012",
        )
        data = yaml.safe_load(result)
        assert data["clusters"][0]["name"] == "my-cluster-123"
        assert data["current-context"] == "my-cluster-123"
