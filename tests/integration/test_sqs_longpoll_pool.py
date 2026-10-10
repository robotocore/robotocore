"""SQS long-poll waits must not run on the default asyncio executor.

ReceiveMessage with WaitTimeSeconds=20 sleeps in a worker thread waiting for
messages. On the default executor (the small fixed pool every `asyncio.to_thread`
shares) a few dozen concurrent long polls pin every worker and queue unrelated
services' work behind them. The provider dispatches long polls onto a dedicated
pool named `sqs-longpoll`; this drives one long poll through the in-process app
and asserts the receive ran on a pool-owned thread.
"""

import threading

import pytest

from robotocore.services.sqs.models import FifoQueue, StandardQueue

_QUEUE_CLASSES = (StandardQueue, FifoQueue)


@pytest.fixture
def watched_receives():
    """Record the thread name each receive runs on, restoring afterwards."""
    seen = []
    originals = {cls: cls.receive for cls in _QUEUE_CLASSES}

    for cls in _QUEUE_CLASSES:
        real = originals[cls]

        def record_then_receive(self, _real=real, *args, **kwargs):
            seen.append(threading.current_thread().name)
            return _real(self, *args, **kwargs)

        cls.receive = record_then_receive

    yield seen

    for cls, fn in originals.items():
        cls.receive = fn


class TestLongPollPool:
    def test_long_poll_runs_on_dedicated_pool(self, make_boto_client, watched_receives):
        sqs = make_boto_client("sqs")
        queue_url = sqs.create_queue(QueueName="lp-pool-probe")["QueueUrl"]

        sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)

        assert watched_receives, "ReceiveMessage must have run"
        assert all(n.startswith("sqs-longpoll") for n in watched_receives), (
            f"expected dedicated long-poll pool threads, saw {watched_receives}"
        )
