"""port sentinel: unit python-era services tests moved to tests/legacy-py-provider.

Rust service backends are unit-tested by cargo suites; this only keeps
`uv run pytest tests/unit/services/` valid for the protected workflow.
"""


def test_port_note() -> None:
    assert True
