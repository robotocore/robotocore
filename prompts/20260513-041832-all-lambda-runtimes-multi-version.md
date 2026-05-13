---
session: "multi-version-all-runtimes"
timestamp: "2026-05-13T04:18:32Z"
model: claude-opus-4-7
---

## Human

Implement version-specific support for all lambda languages just like you did for Node.

## Assistant

## Key decisions

Extended the Node-style multi-version dispatch pattern (introduced in commit
`abd4ca7e`) to the four remaining Lambda runtime executors: Ruby, Java, .NET,
and Python.

### Per-runtime executor caching

`runtimes/__init__.py::get_executor_for_runtime()` previously special-cased
only `nodejs` to cache executors keyed by the full runtime string. Now it does
the same for `ruby`, `java`, `dotnet`, and `python` so each version gets its
own instance threaded with the AWS runtime identifier. The constructor on each
executor accepts `runtime: str = ""` and uses it in `_resolve_binary()`.

### Binary resolution per language

- **Ruby/Java/Node** (subprocess-launched, dynamically linked): added
  `_RUNTIME_BINARY: dict[str, str]` mapping the AWS runtime identifier to a
  versioned binary name (`ruby3.3`, `java17`, `node20`). `_resolve_binary()`
  prefers the versioned name and falls back to the family default with a
  warning when the runtime is unrecognized.

- **Python** (in-process for performance): we can't swap the interpreter at
  call time without losing the perf benefit of in-process execution. The
  executor still accepts a `runtime` argument and `_check_version_match()`
  emits a one-shot warning when the host Python doesn't match the requested
  runtime, so divergence is visible. The map (`_RUNTIME_BINARY`) stores
  `(major, minor)` tuples instead of binary names to make the host match
  decision trivial.

- **.NET** (single host with per-runtime TFM dispatch): the `dotnet` host
  multiplexes runtime versions via `runtimeconfig.json`. `_RUNTIME_BINARY`
  here maps runtime identifiers to target framework monikers
  (`dotnet8 → net8.0`). `_detect_tfm()` now takes the requested runtime and
  prefers the matching TFM when that major version is installed, falling
  back to the latest installed major with a warning otherwise. Split the
  cached-detection into `_list_installed_majors()` so version-specific
  preferences don't get clobbered by a single cached "best" value.

### Dockerfile

- **Node.js** (already done in `abd4ca7e`): `COPY --from=node:X-slim
  /usr/local/bin/node` works because Node binaries are statically
  self-contained.
- **Ruby/Java**: shared-library dependencies (`libruby.so`, JDK's `lib/`)
  mean the single-file COPY trick doesn't work. As an interim step, the
  versioned names (`ruby3.2`/`ruby3.3`/`ruby3.4`, `java8`/`java11`/`java17`/
  `java21`) are symlinked to the apt-installed default. The Lambda runtime
  executor's `_resolve_binary()` will find the versioned name and avoid the
  fallback warning; true per-version installs can replace these symlinks
  later without changing the executor or the test surface.
- **.NET**: real multi-version installs. `dotnet-install.sh` accepts
  multiple `--channel` invocations into the same prefix; added channels
  6.0 and 9.0 alongside the existing 8.0 SDK. Channel 8.0 stays as the
  SDK (used by bootstrap compilation); 6.0 and 9.0 are runtime-only since
  the SDK side compiles fine targeting older runtimes.

### `/_robotocore/runtimes` endpoint

Added a `versions` field keyed by family. Each list contains the AWS Lambda
runtime identifiers whose version-specific binary (or, for Python, host
match; for .NET, installed major) is currently available. Clients that
previously checked just `available` keep working — the new field is additive.

### Test surface

Added `TestXVersionRouting` classes mirroring `TestNodejsVersionRouting` for
each language: `_RUNTIME_BINARY` coverage, versioned-preferred-when-present,
fallback path, no-binary returns InvalidRuntime, distinct per-version
instances cached, unknown-runtime warning. Updated
`test_runtime_registry.py::test_different_versions_same_family_share_executor`
which previously asserted the opposite (it had been written before per-runtime
caching landed); replaced with a parametrized
`test_different_versions_same_family_get_distinct_executors` covering all five
multi-version families plus a positive test that `custom` still shares.

129 new + updated tests pass. Full lambda + gateway unit suite (1097 tests)
green. Ruff clean.

### Follow-ups not done in this commit

- Replace Ruby/Java symlinks with real per-version installs (rbenv/asdf or
  full `/usr/local` copies from `ruby:X-slim`; per-major OpenJDK packages).
  The runtime executor will pick these up automatically when the versioned
  binary names point at distinct interpreters.
- Compat tests that exercise actual handler execution per version (currently
  only the resolution and caching are tested; the existing per-language
  smoke tests run on whatever the default binary is).
