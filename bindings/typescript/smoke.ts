/**
 * Smoke test: drive SQS and S3 through the session helper against a
 * running robotocore twin.
 *
 * Usage:
 *   npm run smoke                                  # twin at http://127.0.0.1:4566
 *   ROBOTOCORE_ENDPOINT=http://127.0.0.1:5000 npm run smoke
 */

import { endpoint, session } from "./robotocore.ts";

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(`smoke: FAILED — ${message}`);
}

async function smoke(): Promise<void> {
  const twin = endpoint();
  const suffix = Date.now().toString(36);
  console.log(`smoke: twin at ${twin}`);

  // SQS: create, inspect, delete a queue.
  const sqs = session().client("sqs");
  const queueName = `robotocore-ts-smoke-${suffix}`;
  const created = await sqs.createQueue({ QueueName: queueName });
  const queueUrl = created.QueueUrl;
  assert(typeof queueUrl === "string", `createQueue returned a QueueUrl (${created.QueueUrl})`);
  assert(queueUrl!.includes(`/${queueName}`), `QueueUrl contains the queue name (${queueUrl})`);

  const sent = await sqs.sendMessage({ QueueUrl: queueUrl!, MessageBody: "hello" });
  assert(typeof sent.MessageId === "string", `sendMessage returned a MessageId (${sent.MessageId})`);

  const received = await sqs.receiveMessage({ QueueUrl: queueUrl!, MaxNumberOfMessages: 1 });
  assert(received.Messages?.[0]?.Body === "hello", `receiveMessage saw the body (${JSON.stringify(received.Messages)})`);

  await sqs.deleteQueue({ QueueUrl: queueUrl! });
  console.log(`smoke: sqs ${queueName} create/send/receive/delete OK`);

  // S3: bucket + object roundtrip through the forcePathStyle client.
  const s3 = session().client("s3");
  const bucket = `robotocore-ts-smoke-${suffix}`;
  await s3.createBucket({ Bucket: bucket }); // us-east-1 needs no location constraint
  await s3.putObject({ Bucket: bucket, Key: "hello.txt", Body: "hello" });
  const fetched = await s3.getObject({ Bucket: bucket, Key: "hello.txt" });
  const body = await fetched.Body?.transformToString();
  assert(body === "hello", `getObject round-tripped the body (${body})`);
  await s3.deleteObject({ Bucket: bucket, Key: "hello.txt" });
  await s3.deleteBucket({ Bucket: bucket });
  console.log(`smoke: s3 ${bucket} put/get/delete OK`);

  console.log("smoke: PASS");
}

smoke().catch((error: unknown) => {
  console.error("smoke:", error);
  process.exitCode = 1;
});
