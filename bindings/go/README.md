# robotocore Go bindings

Thin aws-sdk-go-v2 wrapper that aims clients at a running robotocore
twin. Mirrors `../python/robotocore_client` (endpoint resolution,
per-account credentials); the wire behavior is language-agnostic and
lives server-side.

## API

`robotocore.go` (package `robotocore`) exposes:

| Export | Purpose |
|---|---|
| `Endpoint(override)` | Twin URL: arg, `ROBOTOCORE_ENDPOINT`, then `http://127.0.0.1:4566`; trailing slashes trimmed |
| `Credentials(account)` | `(accessKey, "test")` — the 12-digit access key IS the account id, zero-padded |
| `Session{Endpoint, Region, Account}` | One account/region on the twin; zero value resolves via env + defaults (`Region` defaults to `us-east-1`) |
| `NewSession(service)` | Convenience `Session{}.Client(service)` — returns the `*s3.Client` / `*sqs.Client` as `any` (type-assert, e.g. `.(*sqs.Client)`) |
| `Session.Config(ctx)` | Raw `aws.Config` with `BaseEndpoint`, static credentials and region |
| `Session.Client(service)` | "s3" or "sqs" client; unknown names error |
| `Session.S3Client(ctx)` / `Session.SQSClient(ctx)` | Pre-typed clients; S3 is path-style (`UsePathStyle = true`) |

```go
c, _ := robotocore.NewSession("sqs")
sqs := c.(*sqs.Client)

s3, _ := (&robotocore.Session{Account: "222222222222", Region: "eu-west-1"}).S3Client(ctx)
```

## Smoke test

`main_test.go` holds unit tests plus `TestSmokeLiveTwin`, which
creates/sends/receives/deletes an SQS queue and does an S3
put/get/delete round-trip against a running twin. It skips unless
`ROBOTOCORE_ENDPOINT` is set (and always under `-short`):

```bash
go test -short .                                   # unit tests only, no twin needed
ROBOTOCORE_ENDPOINT=http://127.0.0.1:4566 go test -v .   # unit + live smoke
```
