"""Shared test fixtures for robotocore.

The server under test is the rust binary; tests drive it over HTTP, so the
fixtures here only hand out boto3 clients pointing at the running server.
(Fixtures for the removed in-process python server — `client`,
`reset_state_manager_singleton_*` — are gone; the python server package is
deleted in a later phase and the unit suites that used them go with it.)
"""


@pytest.fixture
def aws_client():
    """boto3-compatible client factory pointing at robotocore."""
    import boto3

    def _make_client(service_name: str, region_name: str = "us-east-1"):
        return boto3.client(
            service_name,
            endpoint_url="http://localhost:4566",
            region_name=region_name,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )

    return _make_client


@pytest.fixture
def live_aws_client():
    """boto3 client factory for integration tests against a running container.

    Only use in tests/integration/ — requires `docker compose up` first.
    """
    import boto3

    def _make_client(service_name: str, region_name: str = "us-east-1"):
        return boto3.client(
            service_name,
            endpoint_url="http://localhost:4566",
            region_name=region_name,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )

    return _make_client
