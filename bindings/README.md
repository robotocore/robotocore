# robotocore language bindings

The server speaks the standard AWS wire protocol, so every AWS SDK works
out of the box: point it at http://127.0.0.1:4566, use any 12-digit access
key as the account id and any non-empty string as the secret.

Each subdirectory here wraps a language SDK with robotocore conveniences
(endpoint resolution, per-test account ids). `python/` is one of them, not
a special case: the server itself contains no Python.
