# Moto Operations Pilot — Progress Tracker (historical)

This pilot drove the pre-port moto era (March 2026): connect, iot, and
glue lifecycle suites were brought to 202/202 passing against the moto
backends, which unlocked their native ports. **Superseded**: the rust
branch ported those flows directly into rotocore; the vendored moto
checkout, `tests/moto_impl/`, and the moto-fork workflow are gone.

## Final baseline (2026-03-17)
- Connect: **58/58 passed** ✓
- IoT:     **66/66 passed** ✓
- Glue:    **78/78 passed** ✓
- **Total: 202/202 passed ✓ GOAL ACHIEVED**

The equivalent lifecycle coverage now lives in `tests/compatibility/`
and the parity lane promoted in `.parity/expectations_current.json`.
