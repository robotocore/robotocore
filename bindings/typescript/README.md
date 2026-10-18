# robotocore TypeScript bindings

Aim AWS SDK v3 at the robotocore server (see ../python for the binding
shape; the typescript variant follows the same options):

```ts
import { S3Client } from "@aws-sdk/client-s3";
const client = new S3Client({
  endpoint: process.env.ROBOTOCORE_ENDPOINT ?? "http://127.0.0.1:4566",
  region: "us-east-1",
  credentials: { accessKeyId: "123456789012", secretAccessKey: "test" },
  forcePathStyle: true,
});
```

Native JS client helpers (endpoint resolution, test account generation)
land here; server behavior is language-agnostic.
