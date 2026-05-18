#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for the per-replica StreamBroadcaster coordinator."""

import asyncio

import pytest

from zenml.zen_server.streaming.broadcaster import (
    StreamBroadcaster,
    StreamCapacityError,
)
from zenml.zen_server.streaming.brokers.base import (
    BrokerConnectionError,
    BrokerEntry,
)
from zenml.zen_server.streaming.brokers.memory import (
    InMemoryBroker,
    InMemoryBrokerSettings,
)


def _run(coro):
    return asyncio.run(coro)


def test_attach_replays_history_then_goes_live():
    """A new consumer replays the broker history before live events arrive."""

    async def scenario():
        broker = InMemoryBroker(settings=InMemoryBrokerSettings(max_len=10))
        broadcaster = StreamBroadcaster(broker=broker, idle_grace_seconds=0.1)
        try:
            await broker.publish("k", [b"a", b"b"])
            agen = await broadcaster.attach("k")
            first = await agen.__anext__()
            second = await agen.__anext__()
            assert isinstance(first, BrokerEntry)
            assert isinstance(second, BrokerEntry)
            assert [first.payload, second.payload] == [b"a", b"b"]
            # Publish a live event and ensure it arrives.
            await broker.publish("k", [b"c"])
            third = await asyncio.wait_for(agen.__anext__(), timeout=2.0)
            assert third.payload == b"c"
            await agen.aclose()
        finally:
            await broadcaster.shutdown()

    _run(scenario())


def test_capacity_cap_enforced():
    """An attach beyond max_consumers_per_stream raises StreamCapacityError."""

    async def scenario():
        broker = InMemoryBroker(settings=InMemoryBrokerSettings(max_len=10))
        broadcaster = StreamBroadcaster(
            broker=broker, max_consumers_per_stream=1, idle_grace_seconds=0.1
        )
        try:
            agen1 = await broadcaster.attach("k")
            # Force agen1 to consume so the session is fully wired.
            consume_task = asyncio.create_task(agen1.__anext__())
            await asyncio.sleep(0.05)
            with pytest.raises(StreamCapacityError):
                await broadcaster.attach("k")
            consume_task.cancel()
            try:
                await consume_task
            except BaseException:
                pass
            await agen1.aclose()
        finally:
            await broadcaster.shutdown()

    _run(scenario())


def test_reader_cancelled_when_last_consumer_detaches():
    """The reader task is cancelled at detach to release its broker connection.

    Keeping the reader alive during the idle-grace window monopolizes a
    Redis connection per idle stream. Under dashboard churn this can
    starve the connection pool. The session itself sticks around so a
    reconnect skips the cursor probe.
    """

    async def scenario():
        broker = InMemoryBroker(settings=InMemoryBrokerSettings(max_len=10))
        broadcaster = StreamBroadcaster(broker=broker, idle_grace_seconds=10.0)
        try:
            await broker.publish("k", [b"a"])
            agen = await broadcaster.attach("k")
            # Iterate once so the generator's try/finally is active.
            await asyncio.wait_for(agen.__anext__(), timeout=2.0)
            session = broadcaster._sessions["k"]
            reader = session.reader_task
            assert reader is not None
            await agen.aclose()
            for _ in range(50):
                if reader.done():
                    break
                await asyncio.sleep(0.01)
            assert reader.done()
            assert session.reader_task is None
            assert (
                "k" in broadcaster._sessions
            )  # session survives the grace window
        finally:
            await broadcaster.shutdown()

    _run(scenario())


def test_reattach_within_grace_resumes_from_cursor():
    """A re-attach within idle grace picks up events published while idle."""

    async def scenario():
        broker = InMemoryBroker(settings=InMemoryBrokerSettings(max_len=10))
        broadcaster = StreamBroadcaster(broker=broker, idle_grace_seconds=10.0)
        try:
            await broker.publish("k", [b"a"])
            agen1 = await broadcaster.attach("k")
            first = await asyncio.wait_for(agen1.__anext__(), timeout=2.0)
            assert first.payload == b"a"
            await agen1.aclose()

            # Reader is gone, but the session and its cursor remain.
            session = broadcaster._sessions["k"]
            assert session.reader_task is None
            cursor_before = session.cursor
            assert cursor_before is not None

            # Publish while the stream is idle.
            await broker.publish("k", [b"b", b"c"])

            # Re-attach — the new reader picks up from session.cursor.
            agen2 = await broadcaster.attach("k", from_id=cursor_before)
            assert session.reader_task is not None
            second = await asyncio.wait_for(agen2.__anext__(), timeout=2.0)
            third = await asyncio.wait_for(agen2.__anext__(), timeout=2.0)
            assert [second.payload, third.payload] == [b"b", b"c"]
            await agen2.aclose()
        finally:
            await broadcaster.shutdown()

    _run(scenario())


def test_reattach_after_grace_creates_new_session(
    monkeypatch: pytest.MonkeyPatch,
):
    """After the grace window expires, re-attach probes the broker again."""

    async def scenario():
        broker = InMemoryBroker(settings=InMemoryBrokerSettings(max_len=10))
        # Tight grace so the test runs fast.
        broadcaster = StreamBroadcaster(broker=broker, idle_grace_seconds=0.05)
        try:
            await broker.publish("k", [b"a"])
            agen1 = await broadcaster.attach("k")
            await asyncio.wait_for(agen1.__anext__(), timeout=2.0)
            await agen1.aclose()

            # Wait past the grace window so the session is torn down.
            for _ in range(50):
                if "k" not in broadcaster._sessions:
                    break
                await asyncio.sleep(0.02)
            assert "k" not in broadcaster._sessions

            # Spy on latest_id to confirm it's invoked on the fresh attach.
            calls = []
            real_latest = broker.latest_id

            async def spy(stream_key):
                calls.append(stream_key)
                return await real_latest(stream_key)

            monkeypatch.setattr(broker, "latest_id", spy)

            agen2 = await broadcaster.attach("k")
            assert calls == ["k"]
            await agen2.aclose()
        finally:
            await broadcaster.shutdown()

    _run(scenario())


def test_initial_cursor_failure_raises_at_attach(
    monkeypatch: pytest.MonkeyPatch,
):
    """If latest_id can't be resolved, attach raises rather than starting a reader.

    Regression for the bug where a broker hiccup made the reader
    cursor=None and then re-broadcast the entire retained history
    to every consumer. The cursor is now resolved synchronously at
    session-create time inside the broadcaster lock. Broker failure there
    surfaces as an exception from `await broadcaster.attach(...)` so the
    SSE endpoint can return a clean 503.
    """

    class FlakyBroker(InMemoryBroker):
        def __init__(self):
            super().__init__(settings=InMemoryBrokerSettings(max_len=10))
            self.fail_latest = True

        async def latest_id(self, stream_key):
            if self.fail_latest:
                raise BrokerConnectionError("simulated broker outage")
            return await super().latest_id(stream_key)

    import zenml.zen_server.streaming.broadcaster as hub_mod

    monkeypatch.setattr(hub_mod, "_INITIAL_CURSOR_RETRIES", 2)
    monkeypatch.setattr(hub_mod, "_RECONNECT_BACKOFF_INITIAL", 0.01)

    async def scenario():
        broker = FlakyBroker()
        broadcaster = StreamBroadcaster(broker=broker, idle_grace_seconds=0.1)
        try:
            with pytest.raises(BrokerConnectionError):
                await broadcaster.attach("k")
        finally:
            await broadcaster.shutdown()

    _run(scenario())


def test_initial_cursor_programmer_error_is_not_retried():
    """Programmer errors (e.g. ValueError) propagate without being retried.

    Previously the cursor probe caught `Exception` and re-raised the
    final attempt as `BrokerConnectionError`. That hid actual bugs in
    the broker implementation behind retry latency and a misleading
    exception type. Only transient connectivity errors are retried now.
    """

    class BuggyBroker(InMemoryBroker):
        def __init__(self):
            super().__init__(settings=InMemoryBrokerSettings(max_len=10))
            self.calls = 0

        async def latest_id(self, stream_key):
            self.calls += 1
            raise ValueError("oops, bug in broker")

    async def scenario():
        broker = BuggyBroker()
        broadcaster = StreamBroadcaster(broker=broker, idle_grace_seconds=0.1)
        try:
            with pytest.raises(ValueError, match="oops"):
                await broadcaster.attach("k")
            # Confirm no retries were attempted.
            assert broker.calls == 1
        finally:
            await broadcaster.shutdown()

    _run(scenario())


def test_shutdown_does_not_leak_delayed_close_tasks():
    """`broadcaster.shutdown` shouldn't spawn lingering close_tasks via `_detach`.

    After shutdown sets `_closing=True`, consumers whose `_stream`
    generators are still draining their queues will run `_detach` in
    their finally blocks. Without the closing-aware guard, those
    detaches would schedule a fresh `_delayed_close` (30s sleep) that
    shutdown never gathered, leaving a pending task at exit and racing
    with `broker.close()`.
    """

    async def scenario():
        broker = InMemoryBroker(settings=InMemoryBrokerSettings(max_len=10))
        broadcaster = StreamBroadcaster(broker=broker, idle_grace_seconds=60.0)
        await broker.publish("k", [b"a"])
        agen = await broadcaster.attach("k")
        # Force the consumer past the registration so its generator
        # holds a live reference to the session.
        await asyncio.wait_for(agen.__anext__(), timeout=2.0)

        await broadcaster.shutdown()

        # Closing the generator now runs `_detach`. With the guard in
        # place, it should NOT create a new close_task.
        await agen.aclose()

        for session in list(broadcaster._sessions.values()):
            assert session.close_task is None or session.close_task.done()

    _run(scenario())


def test_catchup_broker_error_yields_outage_gap():
    """Catchup-time broker failures surface as `outage` (not the removed `broker_error`).

    Documents the L3 collapse: the gap reason on the SSE wire is the
    same for any transient broker problem, since the consumer's
    documented response is identical in both cases (re-fetch from
    durable state).
    """
    from zenml.zen_server.streaming.types import GapMarker, GapReason

    class CatchupFlakyBroker(InMemoryBroker):
        def __init__(self):
            super().__init__(settings=InMemoryBrokerSettings(max_len=10))
            self.read_failed = False

        async def read(self, stream_key, from_id, **kwargs):
            if not self.read_failed:
                self.read_failed = True
                raise BrokerConnectionError("catchup hiccup")
            return await super().read(stream_key, from_id, **kwargs)

    async def scenario():
        broker = CatchupFlakyBroker()
        broadcaster = StreamBroadcaster(broker=broker, idle_grace_seconds=0.1)
        try:
            await broker.publish("k", [b"a"])
            agen = await broadcaster.attach("k", from_id="0")
            first = await asyncio.wait_for(agen.__anext__(), timeout=2.0)
            assert isinstance(first, GapMarker)
            assert first.reason == GapReason.OUTAGE
            await agen.aclose()
        finally:
            await broadcaster.shutdown()

    _run(scenario())
