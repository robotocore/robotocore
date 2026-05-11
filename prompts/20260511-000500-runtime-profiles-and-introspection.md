---
session: "ec77da82"
timestamp: "2026-05-11T00:05:00Z"
model: claude-sonnet-4-6
---

## Human

How should we design our tiers of local and remote tests such that we can guarantee an end user can run any supported language in robotocore?

## Assistant

Proposed three tiers: (1) unit tests prove executor logic unconditionally by mocking subprocess; (2) compat tests always contact the server and fail if the runtime is absent, not skip based on local PATH; (3) Docker smoke tests are the end-user guarantee — if a runtime is in the image, it's smoke tested.

Key design principle: Docker is the only ground truth for end users. Unit and compat tests run against the host environment and can only prove "works where I'm running this."

## Human

And what's the ideal design of this app for that purpose

## Assistant

Three parts: (1) runtime profiles — Dockerfile as composable stages so you can build slim (Python+Node+Ruby) and java-and-dotnet variants; (2) server is self-describing via GET /_robotocore/runtimes — tells clients which families are actually executable, enabling test skip decisions based on server state rather than local PATH; (3) Docker smoke test tier becomes a tautology: everything in the image is smoke tested, the gap is structurally impossible.

## Human

Do it. Add Ruby and create a second, 'java-and-dotnet' docker image. Ensure they're both published and everything is thoroughly tested in CI.

## Assistant

Implementation decisions:

**Dockerfile multi-stage structure**: Named the standard runtime stage `standard` (Python+Node+Ruby) and added `java-and-dotnet` as a target stage that extends it with `openjdk-21-jdk-headless` and `.NET SDK 8` (via the official install script, since apt repos on slim have outdated versions).

**Why javac not just java**: The Java executor compiles `Bootstrap.java` at startup. The JRE alone isn't sufficient; we need the JDK.

**Why dotnet-install.sh not apt**: `apt-get install dotnet-sdk-8.0` on debian-slim requires adding Microsoft's apt repository (fragile, adds complexity). The official install script is cleaner and pins a specific channel.

**`/_robotocore/runtimes` endpoint**: Added to `app.py` at module level so the `shutil` import is patchable in unit tests. Python and custom are unconditional (in-process and /bin/sh respectively). Java requires BOTH `java` and `javac` to be present (bootstrap needs compilation).

**Compat test skip migration**: Replaced all `shutil.which()` skip conditions in ruby/java/dotnet compat tests with `skip_if_runtime_unavailable(family)` from conftest. Added `_server_available_runtimes()` helper that caches the server response per process via `functools.lru_cache`.

**CI smoke test for Java**: The ubuntu-latest runner has Java pre-installed. Compiled a minimal `Handler.java` on the runner, included the `.class` file directly in the Lambda zip (no JAR needed since `tmpdir` itself is on the classpath).

**CI smoke test for .NET**: Used the DotnetExecutor's source compilation mode — passed a `.cs` file in the zip, server compiles it inside the container. Handler format `SmokeTest::Handler::HandleRequest` triggers compilation of `Handler.cs` into `SmokeTest.dll`.

**CI runtimes verification**: Added explicit steps that call `/_robotocore/runtimes` and assert the expected set for each image — standard image must have python/nodejs/ruby/custom but NOT java/dotnet; java-and-dotnet must have all six.
