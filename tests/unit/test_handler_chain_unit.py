"""port sentinel: the python-era handler-chain unit tests moved to tests/legacy-py-provider.

Rust handler-chain code is unit-tested by cargo suites; this only keeps
the `tests/unit/test_handler_chain_unit.py` filename in the unit-gateway
shard valid for the protected workflow.
"""


def test_port_note() -> None:
    assert True
