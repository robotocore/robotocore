---
session: "a01a6d1d"
timestamp: "2026-03-12T05:49:23Z"
model: claude-opus-4-6
---

## Human

Write comprehensive compat tests for Java Lambda execution in Robotocore. Tests should cover: basic handler, map return, JSON event processing, environment variables, error handling, context object, multiple invocations, handler not found, timeout, and built-in classes.

## Assistant

## Key decisions

**Bootstrap protocol**: Read `Bootstrap.java` to understand the invocation protocol. The bootstrap passes events as raw JSON strings for `String` handlers, wraps in a Map with `_raw` key for `Map` handlers. Context is a `Map<String, Object>`, not a real AWS Context.

**Java availability detection**: macOS has stub binaries at `/usr/bin/javac` that exist but fail without a JDK installed. Used `subprocess.run([javac, "-version"])` instead of `shutil.which()` to detect actually-functional Java.

**Test compilation**: Each test compiles Java source via `javac`, packages `.class` files into a zip, and deploys as a Lambda function with `runtime="java21"`.

**19 tests across 8 test classes**: TestJavaBasicInvocation (3), TestJavaMapHandler (2), TestJavaEventProcessing (2), TestJavaEnvironmentVariables (2), TestJavaErrorHandling (3), TestJavaContextObject (1), TestJavaMultipleInvocations (2), TestJavaTimeout (1), TestJavaBuiltinClasses (3).

**Skip strategy**: All tests skip at module level when Java is not functional, so the test suite remains green on machines without a JDK.
