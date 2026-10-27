# robotocore TypeScript bindings

Thin AWS SDK v3 wrapper that aims clients at a running robotocore twin.
Mirrors `../python/robotocore_client` (endpoint resolution, per-account
credentials); the wire behavior is language-agnostic and lives server-side.

## API

`robotocore.ts` exposes:

| Export | Purpose |
|---|---|
| `endpoint(override?)` | Twin URL: arg, `ROBOTOCORE_ENDPOINT`, then `http://127.0.0.1:4566`; trailing slashes trimmed |
| `credentials(account?)` | `{ accessKeyId, secretAccessKey: "test" }` — the 12-digit access key IS the account id, zero-padded |
| `accountFor(region?, accountId?)` | Full client config (`endpoint`, `region`, `credentials`, `forcePathStyle: true`) splattable into any SDK v3 client |
| `session(options?)` | `Session` with `client("s3" | "sqs")` factory + the same fields as `accountFor()` |
| `DEFAULT_ENDPOINT`, `DEFAULT_ACCOUNT` | `http://127.0.0.1:4566` / `123456789012` |

```ts
import { session } from "./robotocore.ts";

const sqs = session().client("sqs"); // SQS from @aws-sdk/client-sqs
const s3 = session({ account: "222222222222", region: "eu-west-1" }).client("s3");
```

`client("s3")` / `client("sqs")` return the SDK's operation-bearing `S3` /
`SQS` classes (they extend `S3Client`/`SQSClient`, so `client.send(cmd)`
works too). No build step: the package ships TypeScript sources and runs
them directly.

## Smoke test

`smoke.ts` creates/sends/receives/deletes an SQS queue and does an S3
put/get/delete round-trip against a running twin:

```bash
npm install
npm run smoke                                    # twin at http://127.0.0.1:4566
ROBOTOCORE_ENDPOINT=http://127.0.0.1:5000 npm run smoke   # any other twin
npm run typecheck                                # tsc --noEmit (strict)
```
