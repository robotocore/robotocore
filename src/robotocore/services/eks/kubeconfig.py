"""Generate kubeconfig YAML for mock EKS clusters."""

import base64

# Fake CA certificate placeholder (not real TLS -- mock server doesn't use TLS)
_FAKE_CA_CERT = (
    b"-----BEGIN CERTIFICATE-----\n"
    b"MIICpDCCAYwCCQDU+pQ4pHgSpDANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAls\n"
    b"b2NhbGhvc3QwHhcNMjQwMTAxMDAwMDAwWhcNMjUwMTAxMDAwMDAwWjAUMRIwEAYD\n"
    b"VQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC7\n"
    b"-----END CERTIFICATE-----\n"
)


def generate_kubeconfig(
    cluster_name: str,
    endpoint: str,
    region: str,
    account_id: str,
) -> str:
    """Return a kubeconfig YAML string pointing to the mock K8s endpoint.

    Args:
        cluster_name: Name of the EKS cluster.
        endpoint: HTTP URL of the mock K8s API server (e.g. http://localhost:12345).
        region: AWS region.
        account_id: AWS account ID.

    Returns:
        YAML string suitable for use with kubectl / client-go.
    """
    ca_data = base64.b64encode(_FAKE_CA_CERT).decode("ascii")

    return f"""\
apiVersion: v1
kind: Config
clusters:
- cluster:
    server: {endpoint}
    certificate-authority-data: {ca_data}
  name: {cluster_name}
contexts:
- context:
    cluster: {cluster_name}
    user: {cluster_name}-user
  name: {cluster_name}
current-context: {cluster_name}
users:
- name: {cluster_name}-user
  user:
    token: k8s-aws-v1.fake-token
"""
