/**
 * robotocore client bindings (TypeScript).
 *
 * Thin wrapper that aims AWS SDK v3 clients at a running robotocore
 * server. Language-agnostic behavior lives server-side; this module only
 * wires credentials/endpoints. Sits alongside the Python, Go, and Rust
 * bindings under `bindings/`, mirroring `../python/robotocore_client`.
 *
 * Usage:
 *   import { session } from "./robotocore.ts";
 *   const sqs = session().client("sqs"); // talks to http://127.0.0.1:4566
 */

import { S3 } from "@aws-sdk/client-s3";
import { SQS } from "@aws-sdk/client-sqs";

export const DEFAULT_ENDPOINT = "http://127.0.0.1:4566";
export const DEFAULT_ACCOUNT = "123456789012";

/** Services bundled with this package's SDK dependencies. */
export type KnownService = "s3" | "sqs";

/** Credentials pair; a 12-digit access key IS the account id. */
export interface Credentials {
  accessKeyId: string;
  secretAccessKey: string;
}

/** Config splattable into any AWS SDK v3 client constructor. */
export interface RobotocoreConfig {
  endpoint: string;
  region: string;
  credentials: Credentials;
  forcePathStyle: true;
}

/** Plain HTTP endpoint for the twin: override, ROBOTOCORE_ENDPOINT, default. */
export function endpoint(override?: string): string {
  const url = override ?? process.env.ROBOTOCORE_ENDPOINT ?? DEFAULT_ENDPOINT;
  return url.replace(/\/+$/, "");
}

/** Credentials for one account: arg, ROBOTOCORE_ACCOUNT, or the default. */
export function credentials(account?: string): Credentials {
  const key = (account ?? process.env.ROBOTOCORE_ACCOUNT ?? DEFAULT_ACCOUNT).padStart(12, "0");
  return { accessKeyId: key, secretAccessKey: "test" };
}

/** Shape for one account/region on the twin. */
export interface AccountOptions {
  region: string;
  credentials: Credentials;
}

/** Per-account/region client config; `forcePathStyle` included for S3. */
export function accountFor(region: string = "us-east-1", accountId?: string): RobotocoreConfig {
  return { endpoint: endpoint(), region, credentials: credentials(accountId), forcePathStyle: true };
}

export interface SessionOptions {
  /** 12-digit account id; defaults to ROBOTOCORE_ACCOUNT then DEFAULT_ACCOUNT. */
  account?: string;
  /** Twin URL; defaults to ROBOTOCORE_ENDPOINT then DEFAULT_ENDPOINT. */
  endpoint?: string;
  /** AWS region; defaults to "us-east-1". */
  region?: string;
}

/**
 * A session configured for a robotocore server. See `session()`.
 *
 * `client()` hands back the operation-bearing `S3`/`SQS` classes from the
 * SDK packages (they extend `S3Client`/`SQSClient`, so `.send(cmd)` works
 * there too). Unknown services throw at runtime — add the matching
 * `@aws-sdk/client-*` package and an arm below to extend.
 */
export class Session {
  readonly endpoint: string;
  readonly region: string;
  readonly credentials: Credentials;
  readonly forcePathStyle: true = true;

  constructor(options: SessionOptions = {}) {
    this.endpoint = endpoint(options.endpoint);
    this.region = options.region ?? "us-east-1";
    this.credentials = credentials(options.account);
  }

  /** Builds a pre-configured SDK client for `service`. */
  client(service: "s3"): S3;
  client(service: "sqs"): SQS;
  client(service: KnownService): S3 | SQS {
    const http = { endpoint: this.endpoint, region: this.region, credentials: this.credentials };
    switch (service) {
      case "s3":
        return new S3({ ...http, forcePathStyle: true });
      case "sqs":
        return new SQS(http);
    }
  }
}

/** A boto3-Session-shaped factory configured for a robotocore server. */
export function session(options: SessionOptions = {}): Session {
  return new Session(options);
}
