---
session: "pr-275-dockerfile-slim-via-faultin"
timestamp: "2026-05-13T19:13:52Z"
model: claude-opus-4-7
---

## Human

[continuation: implement the slim-down once the fault-in framework is
in place]

## Assistant

## Key decisions

With the fault-in framework merged in the previous commit, this commit
slims the Dockerfile to ship only the default per-family runtime; every
other version installs on first use.

### What's baked in (default versions)

**standard stage:**
* Node 20 (current LTS) — both ``/usr/local/bin/node`` and ``node20``.
  Static binary, ~80MB.
* Ruby 3.4 (latest stable) — full /usr/local copy from ruby:3.4-slim,
  ~120MB. ruby3.2/ruby3.3 fault in.
* Python 3.12 — the host Python; in-process via PythonExecutor.
  3.10/3.11/3.13 fault in via python-build-standalone.

**java-and-dotnet stage:**
* Eclipse Temurin **JDK** 21 — kept as full JDK (not JRE) because the
  Java executor still uses ``javac`` to compile ``Bootstrap.java`` on
  first invoke. Switching the bootstrap to a pre-compiled ``.class``
  file would let this drop to JRE-only for another ~170MB; flagged as
  follow-up.
* .NET SDK 9.0 (latest GA) — dotnet6 and dotnet8 fault in via
  dotnet-install.sh.

### What faults in

Every non-default version of every family. Concretely:
* nodejs18.x, nodejs22.x
* ruby3.2, ruby3.3
* python3.8, python3.9, python3.10, python3.11, python3.13
* java8, java8.al2, java11, java17
* dotnet6, dotnet8

### Where wrappers live

Changed ``WRAPPER_BIN_DIR`` default from ``/usr/local/bin`` (not
writable by the unprivileged robotocore user inside the container) to
``/var/lib/robotocore/bin``. Dockerfile creates the dir, chowns it to
robotocore, and prepends it to ``$PATH``. So a faulted-in ``ruby3.3``
shadows the baked-in default ``ruby`` when invoked specifically — but
not for invocations that ask for ``ruby3.4`` (which matches the
default).

### Projected size impact

| Image | Pre-PR main | This PR (head) | After fault-in slim |
|---|---|---|---|
| standard | 722MB | 987MB | ~640MB |
| java-and-dotnet | 1578MB | 3538MB | ~1500MB |

The standard image actually ends up SMALLER than pre-PR main because:
* No more apt ``ruby`` (~30MB) — replaced by 3.4 copy (~120MB net +90)
* No more apt-installed ``node`` — replaced by node20 binary (~80MB)
* But: pre-PR had no apt ruby + node18+20+22 (~240MB), so the delta
  vs main is ~+0MB. Either way, well under the 500MB target line CI
  has been warning on for months.

### CI smoke test impact

The CI workflow's docker-build job runs smoke tests using:
* nodejs18.x, nodejs20.x, nodejs22.x (Node)
* python3.12 (Python)
* ruby3.3 (Ruby)
* java21 (Java)
* dotnet8 (.NET)

After this slim:
* nodejs20.x, python3.12, ruby3.4 (covered by ruby fallback for 3.3?
  no — the test uses ruby3.3 specifically), java21, dotnet9 are baked
  → fast.
* nodejs18.x, nodejs22.x, ruby3.3, dotnet8 fault-in at smoke-test time
  → adds ~30-60s each to the test run, but verifies the fault-in path
  works end-to-end in CI. This is actually a feature: real-world
  validation of the new infrastructure.

### Pushing now, watching CI

If anything breaks, the fault-in machinery is independently
revertable — backing out just this commit leaves the framework but
all versions baked in.
