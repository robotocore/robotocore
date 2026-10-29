"""port sentinel: unit python-era tests moved to tests/legacy-py-provider; rust unit tests
live in cargo suites + compat corpora."""


def test_port_note() -> None:
    """Keeps the unit-infra pytest shard collecting at least one test."""
    assert True
