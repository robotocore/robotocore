# legacy-py-provider — python provider test suite (preserved, not run)

The python-era tests that lived under `tests/unit/`, `tests/integration/`,
`tests/contract/`, `tests/e2e/`, `tests/factories/`, `tests/moto_impl/` and
`tests/compatibility/test_*_compat.py` were written against the python
provider implementation (`robotocore.state.manager`, the moto bridge, the
python gateway app). PR #375 replaced that implementation with the Rust 2.0.0
port (`src/core/`, `rotocore/`, `crates/`), so those suites no longer import
anything that exists.

They are moved here — mirroring their original subpaths — instead of being
deleted:

- `unit/` — python-era unit notebooks (gateway, services, state, audit, …)
- `integration/` — python-era integration harnesses (conftest fixtures gone)
- `compatibility/` — python-provider-era compat suites (separate dir:
  `tests/compatibility/legacy-py-provider/`; see each for provenance)

**This directory is not wired to CI.** The `unit:*` pytest shards enumerate
`tests/unit/...` and never match this path; the compat shards glob top-level
`tests/compatibility/test_[a-z]*.py` names only. It is parked for reference
so the python provider era can still be consulted (fidelity comparisons,
back-porting a fix, digging through moto-era behavior).

The Rust port's actual coverage:

- unit: cargo test suites (`rotocore/`, `crates/`, `src/`)
- integration/compat: the rust-era corpora under `tests/compatibility/`
  and `tests/parity/`
