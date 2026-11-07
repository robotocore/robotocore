"""Stateful property-based fuzzing over the twin's SQS and S3.

Hypothesis RuleBasedStateMachine drives random valid operation sequences
and checks invariants NO scenario test thinks to try together: a message
in flight is never delivered to a second receive, deleted messages never
come back, and the S3 listing always equals the put/delete history.

Deterministic about timing (receipt handles are tracked from the receive
that delivered them — no sleeps), so failures are reproducible. Bounded
examples keep the shard cheap; every example uses a fresh account id for
isolation from the other workers.
"""

import uuid

import boto3
import pytest
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from tests.metamorphic.conftest import ENDPOINT_URL, fresh_account

pytestmark = [pytest.mark.metamorphic]


def make_client(service: str, account: str):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id=account,
        aws_secret_access_key="fuzz",
    )


class SQSMachine(RuleBasedStateMachine):
    """Random send/receive/delete sequences on one queue, long visibility
    so nothing expires mid-run — every transition is therefore explicit."""

    def __init__(self):
        super().__init__()
        self.account = fresh_account()
        self.sqs = make_client("sqs", self.account)
        self.queue_url = self.sqs.create_queue(
            QueueName=f"fuzz-{uuid.uuid4().hex[:10]}",
            Attributes={"VisibilityTimeout": "3600"},
        )["QueueUrl"]
        self.unsent_bodies = iter(range(1000))
        self.visible: set[str] = set()
        # body -> receipt handle from the delivering receive
        self.in_flight: dict[str, str] = {}
        self.deleted: set[str] = set()

    @rule(body=st.text(min_size=1, max_size=20, alphabet="ab"))
    def send(self, body: str):
        tagged = f"{body}-{next(self.unsent_bodies)}"
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=tagged)
        self.visible.add(tagged)

    @precondition(lambda self: self.visible)
    @rule(data=st.data())
    def receive_one(self, data):
        wanted = data.draw(st.sampled_from(sorted(self.visible)))
        resp = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=10,
            MessageAttributeNames=["All"],
        )
        msgs = resp.get("Messages", [])
        assert msgs, "a visible message existed but the receive came back empty"
        delivered = [m["Body"] for m in msgs]
        if len(self.visible) <= 10:
            # A receive returns at most 10 messages: with more than 10
            # visible, `wanted` may legitimately miss the page.
            assert wanted in delivered, f"{wanted!r} was visible but never delivered"
        self._absorb(msgs)

    def _absorb(self, msgs: list[dict]):
        """Every delivered message moves visible→in flight — ANY receive
        takes visibility on what it returns (real SQS semantics); the
        model records it wherever the receive happens."""
        for m in msgs:
            body = m["Body"]
            assert body not in self.in_flight, (
                f"{body!r} delivered while its previous receive was still "
                f"in flight — visibility timeout broken"
            )
            assert body not in self.deleted, f"{body!r} was deleted and came back"
            self.visible.discard(body)
            self.in_flight[body] = m["ReceiptHandle"]

    @precondition(lambda self: self.in_flight)
    @rule(data=st.data())
    def delete_in_flight(self, data):
        body = data.draw(st.sampled_from(sorted(self.in_flight)))
        # A verification receive may legitimately deliver OTHER visible
        # messages (they move to in flight); none of the in-flight set
        # may appear.
        resp = self.sqs.receive_message(QueueUrl=self.queue_url, MaxNumberOfMessages=10)
        self._absorb(resp.get("Messages", []))
        self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=self.in_flight.pop(body))
        self.deleted.add(body)

    @invariant()
    def no_double_state(self):
        overlap = set(self.in_flight) & self.deleted
        assert not overlap
        assert not (set(self.in_flight) & self.visible)


TestSQSFuzz = pytest.mark.metamorphic(SQSMachine.TestCase)


class S3Machine(RuleBasedStateMachine):
    """Random put/delete/list sequences on one bucket; the listing must
    always equal the history and GET must return the last PUT's bytes."""

    def __init__(self):
        super().__init__()
        self.account = fresh_account()
        self.s3 = make_client("s3", self.account)
        self.bucket = f"fuzz-{uuid.uuid4().hex[:10]}"
        self.s3.create_bucket(Bucket=self.bucket)
        self.objects: dict[str, bytes] = {}
        self.version = 0

    @rule(key=st.sampled_from(["a", "b/c", "b/d", "e/f/g", "h"]), payload=st.binary(max_size=50))
    def put(self, key: str, payload: bytes):
        self.version += 1
        body = payload + str(self.version).encode()
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)
        self.objects[key] = body

    @precondition(lambda self: self.objects)
    @rule(data=st.data())
    def get_matches_last_put(self, data):
        key = data.draw(st.sampled_from(sorted(self.objects)))
        got = self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        assert got == self.objects[key], f"GET {key!r} returned stale/corrupt bytes"

    @precondition(lambda self: self.objects)
    @rule(data=st.data())
    def delete(self, data):
        key = data.draw(st.sampled_from(sorted(self.objects)))
        self.s3.delete_object(Bucket=self.bucket, Key=key)
        del self.objects[key]

    @invariant()
    def listing_equals_history(self):
        listed = {o["Key"] for o in self.s3.list_objects_v2(Bucket=self.bucket).get("Contents", [])}
        assert listed == set(self.objects), (
            f"listing drifted from history: extra={listed - set(self.objects)} "
            f"missing={set(self.objects) - listed}"
        )


TestS3Fuzz = pytest.mark.metamorphic(S3Machine.TestCase)
