"""port sentinel: unit python-era providers tests moved to tests/legacy-py-provider.

Rust providers are unit-tested by cargo suites; this only keeps
`uv run pytest tests/unit/providers/` valid for the protected workflow.
"""


def test_port_note() -> None:
    assert True
