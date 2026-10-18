# robotocore python bindings

Thin boto3 wrapper: `pip install robotocore-bindings` (or copy the
`robotocore_client` package). See `__init__.py` for the full surface:
`session()`, `client("s3")`, `credentials(account_id)`.

Every other language binding aims the same wire API; the testing rig in
`tests/` exercises the server through boto3 and doubles as the canonical
wire reference for other bindings.
