"""port sentinel: unit python-era protocols tests moved to tests/legacy-py-provider.

Rust protocol handling is unit-tested by cargo suites; this only keeps
`uv run pytest tests/unit/protocols/` valid for the protected workflow.
"""


def test_port_note() -> None:
    assert True
