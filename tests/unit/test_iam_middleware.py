"""port sentinel: the python-era IAM middleware unit tests moved to tests/legacy-py-provider.

Rust middleware is unit-tested by cargo suites; this only keeps the
`tests/unit/test_iam_middleware.py` filename in the unit-gateway shard
valid for the protected workflow.
"""


def test_port_note() -> None:
    assert True
