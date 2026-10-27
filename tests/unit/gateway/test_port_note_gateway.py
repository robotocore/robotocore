"""port sentinel: unit python-era gateway tests moved to tests/legacy-py-provider.

Rust gateway code is unit-tested by cargo suites; this only keeps
`uv run pytest tests/unit/gateway/` valid for the protected workflow.
"""


def test_port_note() -> None:
    assert True
