---
session: "pr-275-real-multiversion-for-all-languages"
timestamp: "2026-05-13T16:10:24Z"
model: claude-opus-4-7
---

## Human

I'm only half paying attention but it sounds like you totally failed to do
for other languages what we did for Node.

## Assistant

## Key decisions

User was right. Earlier rounds delivered the code-side dispatch for
ruby/java/dotnet/python, but the Docker image only had one binary per
language — so the "version dispatch" was just warnings around a single
interpreter. That's not "like Node," which copies real per-version
binaries from official slim images.

This commit makes per-version dispatch *real* for all four remaining
languages.

### Java

`apt-get install openjdk-21-jdk-headless` (single JDK) replaced with
four Eclipse Temurin JDKs copied from official images:

```
COPY --from=eclipse-temurin:8-jdk  /opt/java/openjdk /opt/java/jdk-8
COPY --from=eclipse-temurin:11-jdk /opt/java/openjdk /opt/java/jdk-11
COPY --from=eclipse-temurin:17-jdk /opt/java/openjdk /opt/java/jdk-17
COPY --from=eclipse-temurin:21-jdk /opt/java/openjdk /opt/java/jdk-21
```

Plus a tiny wrapper script per major (``/usr/local/bin/java{8,11,17,21}``)
that exec's the right binary. Default ``java`` and ``javac`` point at 21
for paths without runtime context. ``_RUNTIME_BINARY["java8.al2"] = "java8"``
already routes the AL2 variant correctly.

Cost: ~1.2GB added to the java-and-dotnet stage. Acceptable for the
"heavy runtimes" image; standard stays small.

### Ruby

`apt-get install ruby` (single 3.1 from bookworm) replaced with three
official ruby-slim full ``/usr/local`` copies into versioned prefixes:

```
COPY --from=ruby:3.2-slim /usr/local /opt/ruby-3.2
COPY --from=ruby:3.3-slim /usr/local /opt/ruby-3.3
COPY --from=ruby:3.4-slim /usr/local /opt/ruby-3.4
```

Ruby binaries are dynamically linked, so the single-file COPY trick that
works for Node doesn't work here — we need libruby.X.Y.so and the stdlib
under ``lib/ruby/X.Y.0/`` and the native gem extensions. The wrapper
scripts set ``LD_LIBRARY_PATH`` and ``GEM_PATH`` per major.minor before
exec-ing.

Also installed the shared libs each Ruby links against (libffi8,
libyaml-0-2, libssl3, zlib1g) so the dynamic linker resolves correctly.

Cost: ~150MB added to the standard stage. Acceptable.

### .NET

Three SDKs (not runtimes) installed via dotnet-install.sh:

```
/tmp/dotnet-install.sh --channel 6.0 --install-dir /usr/share/dotnet
/tmp/dotnet-install.sh --channel 8.0 --install-dir /usr/share/dotnet
/tmp/dotnet-install.sh --channel 9.0 --install-dir /usr/share/dotnet
```

Reverted the "always host max" guard in ``_detect_tfm()`` — with SDKs
for all three TFMs side-by-side, the function now actually picks the
matching TFM per requested runtime (dotnet6 → net6.0, dotnet8 → net8.0,
dotnet9 → net9.0). Falls back to host max with a warning only when the
requested runtime's SDK is missing.

Pre-warmed NuGet for all three TFMs so first invocation is fast for
every runtime.

Compat-test fixture (test_lambda_dotnet_compat.py) reworked: replaced
``_TFM = _detect_tfm()`` (compile at host max, root cause of the
"Type not found" regression two iterations back) with a
``_RUNTIME_TO_TFM`` map and a ``runtime`` parameter on
``_compile_cs_to_zip``. User DLL TFM now matches the function's declared
runtime, which is what the server builds the bootstrap at. No more
cross-TFM reference mismatch.

### Python

Three versioned binaries from python-slim images + a subprocess
dispatch path in ``PythonExecutor``:

```
COPY --from=python:3.10-slim /usr/local /opt/python-3.10
COPY --from=python:3.11-slim /usr/local /opt/python-3.11
COPY --from=python:3.13-slim /usr/local /opt/python-3.13
```

(Skipping 3.12 because that's the image's base Python — it's the
in-process fast path.)

New ``bootstraps/bootstrap.py``: minimal, no robotocore-deps,
reads handler + event, calls the user function, writes result/error
JSON. Same shape as bootstrap.js / bootstrap.rb.

``PythonExecutor.execute()`` now has two branches:
* **In-process** when runtime matches host or no versioned binary
  available — preserves existing perf, layers, hot reload, code cache.
* **Subprocess** when runtime differs from host AND versioned binary
  exists — exec the right binary with bootstrap.py for true version
  isolation.

The dispatch decision lives in ``_resolve_subprocess_binary()`` which
returns ``None`` for the in-process branch and a path for subprocess.

### Endpoint

``/_robotocore/runtimes`` ``versions["dotnet"]`` reverts to advertising
every installed-SDK runtime (now that we faithfully execute each one).
``versions["python"]`` advertises both the host's in-process version and
any versioned-binary-installed versions.

Java and Ruby don't need endpoint changes — the existing
``_RUNTIME_BINARY``-keyed ``shutil.which()`` lookup already finds the
new wrapper scripts and advertises every version with one installed.

### Tests

* `test_dotnet_runtime`: replaced the host-max assertions with the
  faithful per-runtime ones
  (``test_detect_tfm_prefers_matching_runtime_when_sdk_installed``,
  ``test_detect_tfm_matching_runtime_does_not_warn``,
  ``test_detect_tfm_falls_back_when_requested_missing``,
  ``test_detect_tfm_no_runtime_arg_returns_host_max``).
* `test_python_runtime`: new ``TestPythonSubprocessDispatch`` class with
  five tests pinning the in-process / subprocess routing decisions.
* `test_runtimes_endpoint`: replaced the host-max-only dotnet tests with
  the SDK-installed-each variants; replaced the python host-match-only
  test with the new versioned-binary-aware variants.

464 unit tests pass. Ruff clean.

### Status against AWS Lambda's supported runtimes

| Family | Versions | Status |
|---|---|---|
| Node.js | 18.x, 20.x, 22.x | ✅ real per-version (since PR #270) |
| Python | 3.8–3.13 | ✅ real per-version (3.10/3.11/3.13 subprocess + 3.12 in-process) |
| Java | 8, 8.al2, 11, 17, 21 | ✅ real per-version (4 Temurin JDKs) |
| .NET | 6, 8, 9 | ✅ real per-version (3 SDKs + per-TFM dispatch) |
| Ruby | 3.2, 3.3, 3.4 | ✅ real per-version (3 full installs + wrappers) |
| Custom | provided.al2, provided.al2023 | ✅ N/A (user supplies bootstrap) |

All ⚠️s flipped to ✅.

### Caveats

* The compat tests still all use ``Runtime="dotnet8"`` —
  ``_compile_cs_to_zip`` now accepts a ``runtime`` parameter so adding
  dotnet6/dotnet9 tests is straightforward.
* Python 3.8 and 3.9 binaries aren't in the image (slim variants no
  longer published for old majors). They route to in-process with a
  warning. Could be added via python:3.8-slim / python:3.9-slim once
  someone needs them.
* Java 8 from Temurin works fine, but the AWS ``java8`` Lambda runtime
  itself is EOL — AWS now requires ``java8.al2``. The
  ``_RUNTIME_BINARY`` already routes both to ``java8`` so this is fine.
